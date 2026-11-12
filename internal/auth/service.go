package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/ids"
	"github.com/mash-cloud/mash-api/internal/observability"
	"github.com/mash-cloud/mash-api/internal/store"
	"github.com/mash-cloud/mash-api/internal/store/repos"
)

// Narrow storage seams: the service depends on behavior, not concrete repos
// (and gains an in-memory fake for rotation/reuse regression tests).
type UsersStore interface {
	ByID(ctx context.Context, id string) (*domain.User, error)
	TouchLogin(ctx context.Context, id string) error
}

type TenantsStore interface {
	ByID(ctx context.Context, id string) (*domain.Tenant, error)
	Membership(ctx context.Context, tenantID, userID string) (*domain.Membership, error)
	Memberships(ctx context.Context, userID string) ([]domain.Membership, error)
}

type RefreshStore interface {
	Consume(ctx context.Context, tokenHash string) (*repos.RefreshToken, bool, error)
	ByHash(ctx context.Context, tokenHash string) (*repos.RefreshToken, error)
	// Rotate atomically consumes the presented token and installs its
	// successor in ONE transaction (audit M14).
	Rotate(ctx context.Context, tokenHash string, successor *repos.RefreshToken) (*repos.RefreshToken, error)
	RevokeFamily(ctx context.Context, familyID, reason string) (int64, error)
	RevokeUser(ctx context.Context, userID, reason string) (int64, error)
	Issue(ctx context.Context, t *repos.RefreshToken) error
}

// Service implements the auth flows against PostgreSQL + Redis. Google OAuth
// is the only way an identity enters the system; the desktop obtains its
// token pair via the web-to-desktop handshake (see desktop_code.go).
type Service struct {
	Users       UsersStore
	Tenants     TenantsStore
	RefreshRepo RefreshStore
	Redis       redis.UniversalClient

	// Google OAuth + web-to-desktop handshake (nil/sealed = disabled).
	Google         *GoogleProvider // nil → OAuth endpoints 503
	OAuth          OAuthStore      // identity provisioning seam
	Devices        DevicesStore    // device registration on exchange
	WebSessionTTL  time.Duration   // short browser token (no refresh)
	WebGrantTTL    time.Duration
	OAuthStateTTL  time.Duration
	DesktopCodeTTL time.Duration

	Signer          *LocalSigner
	AccessTTL       time.Duration
	RefreshTTL      time.Duration
	RevocationCheck bool
}

// TokenPair is the desktop-exchange/refresh response payload.
type TokenPair struct {
	AccessToken  string     `json:"access_token"`
	TokenType    string     `json:"token_type"`
	ExpiresIn    int64      `json:"expires_in"`
	RefreshToken string     `json:"refresh_token"`
	User         UserInfo   `json:"user"`
	Tenant       TenantInfo `json:"tenant,omitempty"`

	// RefreshID is the server-side id of the issued refresh token
	// (for linking rotations; not part of the client contract).
	RefreshID string `json:"-"`
}

// Refresh rotates a refresh token. Reuse of a consumed or revoked token
// revokes the whole family (theft response); a merely EXPIRED token is a
// clean re-login and never a purge (audit M5). Rotation state changes commit
// atomically (audit M14).
func (s *Service) Refresh(ctx context.Context, refreshToken, userAgent string) (*TokenPair, error) {
	if refreshToken == "" {
		return nil, domain.ErrValidation("refresh_token is required")
	}
	tokenHash := HashToken(refreshToken)

	// Phase 1 — read-only context load. Nothing persists until Rotate
	// commits, so any failure here (or a pod crash mid-flow) leaves the
	// presented token exactly as it was: retryable, never stranded (M14).
	rt, err := s.RefreshRepo.ByHash(ctx, tokenHash)
	if err != nil {
		return nil, store.MapDBError(err)
	}
	if rt == nil {
		return nil, domain.ErrUnauthorized(errors.New("refresh token invalid"))
	}

	// Lookup user + membership context for the new access token.
	u, err := s.Users.ByID(ctx, rt.UserID)
	if err != nil || u == nil || u.Status != "active" {
		return nil, &domain.Error{Code: "ACCOUNT_INACTIVE", Message: "Account is not active.", HTTP: 401}
	}
	tenantID, role := rt.TenantID, ""
	if tenantID != "" {
		if m, err := s.Tenants.Membership(ctx, tenantID, u.ID); err == nil && m != nil && m.Status == "active" {
			role = m.Role
		}
	}
	if role == "" {
		// Fall back to any active membership.
		ms, err := s.Tenants.Memberships(ctx, u.ID)
		if err != nil || len(ms) == 0 {
			return nil, domain.ErrForbidden("user has no active tenant membership")
		}
		tenantID, role = ms[0].TenantID, ms[0].Role
	}

	// Phase 2 — mint the successor pair in the same family WITHOUT persisting
	// anything. The successor inherits rt.FamilyID so every token in the chain
	// shares one family id — reuse detection revokes that family and kills the
	// whole chain, including an attacker's derived successors (RFC 6819
	// §5.2.2.3 theft response).
	pair, successor, err := s.mintPair(ctx, u, tenantID, role, rt.DeviceID, userAgent, rt.FamilyID)
	if err != nil {
		return nil, err
	}

	// Phase 3 — atomic rotation: consume the old token, link it to the
	// successor and insert the successor in ONE transaction. The old flow
	// ran three independent transactions (consume → issue → link); a crash
	// between them stranded the session with no valid token, and the
	// stranded state read as THEFT on the next attempt — family-wide logout
	// for exactly the users mid-rotation (audit M14).
	if _, err := s.RefreshRepo.Rotate(ctx, tokenHash, successor); err != nil {
		if status, reuse := repos.ReuseStatus(err); reuse {
			if status == "expired" {
				// Audit M5: a never-used token past its lifetime is a
				// sleeping laptop or a clock-skewed phone, not theft. Clean
				// 401 + fresh sign-in — the old behaviour purged the whole
				// family (every device logged out) on this signal.
				return nil, &domain.Error{
					Code:    "REFRESH_TOKEN_EXPIRED",
					Message: "The refresh token has expired. Please sign in again.",
					HTTP:    401,
				}
			}
			// used / revoked → theft response: revoke family, log loudly, deny.
			if _, rerr := s.RefreshRepo.RevokeFamily(ctx, rt.FamilyID, "reuse"); rerr != nil {
				slog.ErrorContext(ctx, "auth: family revocation failed", "error", rerr)
			} else {
				slog.WarnContext(ctx, "auth: refresh token reuse detected; family revoked",
					"user_id", rt.UserID, "family", rt.FamilyID)
			}
			return nil, &domain.Error{
				Code:    "REFRESH_TOKEN_REUSED",
				Message: "Refresh token reuse detected. All sessions revoked.",
				HTTP:    401,
			}
		}
		return nil, store.MapDBError(err)
	}
	return pair, nil
}

// Logout revokes one refresh token (and blacklists the presented access token
// jti when revocation checking is on).
func (s *Service) Logout(ctx context.Context, refreshToken, accessJTI string, accessTTL time.Duration) error {
	if refreshToken != "" {
		if _, _, err := s.RefreshRepo.Consume(ctx, HashToken(refreshToken)); err != nil {
			if !repos.IsReuseSignal(err) {
				return store.MapDBError(err)
			}
		}
	}
	s.blacklistJTI(ctx, accessJTI, accessTTL)
	return nil
}

// LogoutAll revokes EVERY live refresh-token family for the user (stolen or
// lost device: sign out everywhere) and blacklists the presented access token.
// Within one access-token TTL (15 min by default) all other devices' access
// tokens also die at their next refresh — every family is revoked.
func (s *Service) LogoutAll(ctx context.Context, userID, accessJTI string, accessTTL time.Duration) (int64, error) {
	n, err := s.RefreshRepo.RevokeUser(ctx, userID, "logout_all")
	if err != nil {
		return 0, store.MapDBError(err)
	}
	s.blacklistJTI(ctx, accessJTI, accessTTL)
	return n, nil
}

// blacklistJTI denies the presented access token for its remaining lifetime
// when central revocation checking is enabled (no-op otherwise).
func (s *Service) blacklistJTI(ctx context.Context, accessJTI string, accessTTL time.Duration) {
	if !s.RevocationCheck || accessJTI == "" || s.Redis == nil {
		return
	}
	ttl := accessTTL
	if ttl <= 0 {
		ttl = s.AccessTTL
	}
	if ttl > 0 {
		s.Redis.Set(ctx, "auth:blacklist:"+accessJTI, "1", ttl)
	}
}

// IsBlacklisted consults the jti blacklist.
//
// Fail-open posture (audit M2, now explicit): when Redis errors, the check
// is skipped and the token is honoured for the rest of its (≤15 min) life.
// This is a deliberate availability trade-off — the alternative locks every
// user out during a Redis blip — but it must not be silent: every miss
// widens the revocation window the product promises to shorten, so failures
// are counted on mash_auth_revocation_failopen_total and logged (rate
// limited) as security events.
func (s *Service) IsBlacklisted(ctx context.Context, jti string) bool {
	if jti == "" || s.Redis == nil {
		return false
	}
	n, err := s.Redis.Exists(ctx, "auth:blacklist:"+jti).Result()
	if err != nil {
		revocationFailOpen(ctx, err)
		return false
	}
	return n > 0
}

// revocationFailOpen makes the degraded window visible: a counter metric
// always, a warn log at most once per 10 s per process (a Redis outage must
// not become a log flood).
var revFailOpen struct {
	mu   sync.Mutex
	last time.Time
}

func revocationFailOpen(ctx context.Context, err error) {
	if m := observability.AuthRevocationFailOpen(); m != nil {
		m.Add(ctx, 1)
	}
	revFailOpen.mu.Lock()
	logIt := time.Since(revFailOpen.last) > 10*time.Second
	if logIt {
		revFailOpen.last = time.Now()
	}
	revFailOpen.mu.Unlock()
	if logIt {
		slog.WarnContext(ctx, "auth: revocation check FAILED OPEN — revoked tokens honoured until their expiry",
			"error", err)
	}
}

// --- internals -------------------------------------------------------------

// mintPair builds a token pair WITHOUT persisting the refresh row: callers
// either persist via RefreshStore.Rotate (rotation — atomic consume+link+
// insert, audit M14) or via RefreshStore.Issue (fresh desktop exchange).
// family is the family the refresh token joins; "" starts a fresh family
// rooted at the new token.
func (s *Service) mintPair(ctx context.Context, u *domain.User, tenantID, role, deviceID, userAgent, family string) (*TokenPair, *repos.RefreshToken, error) {
	if s.Signer == nil {
		return nil, nil, &domain.Error{
			Code: "UNSUPPORTED_AUTH_MODE", Message: "This deployment mints no access tokens; use your identity provider.",
			HTTP: http.StatusNotImplemented,
		}
	}
	jti := ids.New("jti")
	access, err := s.Signer.Sign(u.ID, tenantID, role, deviceID, jti, time.Now())
	if err != nil {
		return nil, nil, domain.ErrInternal(err)
	}

	// Opaque refresh secret: 32 bytes entropy.
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, nil, domain.ErrInternal(err)
	}
	refreshSecret := "nxr_" + base64.RawURLEncoding.EncodeToString(secret)

	refreshID := ids.RefreshTokenID()
	if family == "" {
		// Exchange: a fresh family rooted at this token.
		family = refreshID
	}

	rt := &repos.RefreshToken{
		ID:        refreshID,
		UserID:    u.ID,
		TenantID:  tenantID,
		DeviceID:  deviceID,
		FamilyID:  family,
		TokenHash: HashToken(refreshSecret),
		ExpiresAt: time.Now().Add(s.RefreshTTL),
		UserAgent: sanitizeUA(userAgent),
	}

	tenantInfo := TenantInfo{}
	if tenantID != "" {
		if t, _ := s.Tenants.ByID(ctx, tenantID); t != nil {
			tenantInfo = TenantInfo{ID: t.ID, Slug: t.Slug, Name: t.Name, Status: t.Status}
		}
	}

	return &TokenPair{
		AccessToken:  access,
		TokenType:    "Bearer",
		ExpiresIn:    int64(s.AccessTTL.Seconds()),
		RefreshToken: refreshSecret,
		User: UserInfo{
			ID: u.ID, Email: u.Email, DisplayName: u.DisplayName, AvatarURL: u.AvatarURL,
			Status: u.Status, IsPlatformAdmin: u.IsPlatformAdmin,
			EmailVerified: u.EmailVerified,
		},
		Tenant:    tenantInfo,
		RefreshID: refreshID,
	}, rt, nil
}

// issueTokens mints AND persists a fresh pair (desktop exchange path: a new
// family, single INSERT — a crash before it simply means the client retries
// the exchange).
func (s *Service) issueTokens(ctx context.Context, u *domain.User, tenantID, role, deviceID, userAgent, family string) (*TokenPair, error) {
	pair, rt, err := s.mintPair(ctx, u, tenantID, role, deviceID, userAgent, family)
	if err != nil {
		return nil, err
	}
	if err := s.RefreshRepo.Issue(ctx, rt); err != nil {
		return nil, store.MapDBError(err)
	}
	return pair, nil
}

func sanitizeUA(ua string) string {
	return truncateUTF8(ua, 256)
}
