// Package tests — payment_journeys_test.go (user-journey audit, phase 2).
//
// THE PAYMENTS FAMILY (SCENARIOS.md P01-P23): every journey a buyer can
// take, driven against the REAL payment.Service — real checkout idempotency,
// real 3-legged confirm verification, real webhook dedupe/converge/ordering
// rules, real state machine — with hermetic counterparts: MockProvider
// (which runs the REAL HMAC signature paths) and an in-memory Store that
// mirrors the exact SQL semantics documented on payment.Store (conditional
// state transitions, extend-or-insert subscription activation, event-id
// dedupe).
//
// What this file pins (ids reference SCENARIOS.md rows):
//
//      P01-P05  catalog; checkout happy; idempotent replay; key reuse;
//               unknown SKU / unconfigured plan
//      P06      checkout retry-storm budget
//      P07      provider echoing a tampered amount
//      P08-P12  confirm: happy 3-legged, cross-tenant 404, webhook-won
//               race, forged signature, order/payment mismatch
//      P13-P18  webhooks: happy, duplicate converge, out-of-order,
//               forgery, amount tamper, orphan, refund (no reversal),
//               IP flood budget
//      P19/P21  abandoned checkout expiry; late capture on an EXPIRED order
//      P22      failed payment, then a clean retry
package tests

import (
        "context"
        "encoding/json"
        "fmt"
        "strings"
        "sync"
        "testing"
        "time"

        "github.com/alicebob/miniredis/v2"
        "github.com/redis/go-redis/v9"

        "github.com/mash-cloud/mash-api/internal/domain"
        "github.com/mash-cloud/mash-api/internal/observability"
        "github.com/mash-cloud/mash-api/internal/payment"
        "github.com/mash-cloud/mash-api/internal/ratelimit"
)

// ---------------------------------------------------------------------------
// the fake Store: payment.Store semantics, SQL-faithful
// ---------------------------------------------------------------------------

type fakeSubRow struct {
        planCode   string
        status     string // active | canceled | expired
        periodEnd  time.Time
        periodDays int
}

type fakePayStore struct {
        mu        sync.Mutex
        byID      map[string]*payment.Order
        byProvider map[string]*payment.Order // provider|providerOrderID
        byIdem    map[string]*payment.Order  // tenant|idemKey
        subs      map[string]*fakeSubRow     // tenantID
        events    map[string]bool            // webhook event ids
        plans     map[string]bool            // plan_code → exists
        orderSeq  int
}

func newFakePayStore() *fakePayStore {
        return &fakePayStore{
                byID: map[string]*payment.Order{}, byProvider: map[string]*payment.Order{},
                byIdem: map[string]*payment.Order{}, subs: map[string]*fakeSubRow{},
                events: map[string]bool{}, plans: map[string]bool{"pro": true, "free": true},
        }
}

func (s *fakePayStore) CreateOrder(_ context.Context, o *payment.Order) error {
        s.mu.Lock()
        defer s.mu.Unlock()
        if o.IdempotencyKey != "" {
                if _, dup := s.byIdem[o.TenantID+"|"+o.IdempotencyKey]; dup {
                        return payment.ErrOrderExistsIdem
                }
        }
        if _, dup := s.byProvider[o.Provider+"|"+o.ProviderOrderID]; dup {
                return payment.ErrOrderExistsProvider
        }
        s.byID[o.ID] = o
        s.byProvider[o.Provider+"|"+o.ProviderOrderID] = o
        if o.IdempotencyKey != "" {
                s.byIdem[o.TenantID+"|"+o.IdempotencyKey] = o
        }
        return nil
}

func (s *fakePayStore) OrderByID(_ context.Context, id string) (*payment.Order, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        if o, ok := s.byID[id]; ok {
                cp := *o
                return &cp, nil
        }
        return nil, payment.ErrNotFound
}

func (s *fakePayStore) OrderByProviderID(_ context.Context, provider, providerOrderID string) (*payment.Order, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        if o, ok := s.byProvider[provider+"|"+providerOrderID]; ok {
                cp := *o
                return &cp, nil
        }
        return nil, payment.ErrNotFound
}

func (s *fakePayStore) OrderByIdempotencyKey(_ context.Context, tenantID, key string) (*payment.Order, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        if o, ok := s.byIdem[tenantID+"|"+key]; ok {
                cp := *o
                return &cp, nil
        }
        return nil, payment.ErrNotFound
}

func (s *fakePayStore) ListOrders(_ context.Context, tenantID string, limit int) ([]*payment.Order, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        var out []*payment.Order
        for _, o := range s.byID {
                if o.TenantID == tenantID {
                        cp := *o
                        out = append(out, &cp)
                }
        }
        // newest first (good enough for the bounded test sizes)
        for i := 0; i < len(out); i++ {
                for j := i + 1; j < len(out); j++ {
                        if out[j].CreatedAt.After(out[i].CreatedAt) {
                                out[i], out[j] = out[j], out[i]
                        }
                }
        }
        if len(out) > limit {
                out = out[:limit]
        }
        return out, nil
}

func (s *fakePayStore) MarkAttempted(_ context.Context, orderID, providerPaymentID string) error {
        s.mu.Lock()
        defer s.mu.Unlock()
        if o, ok := s.byID[orderID]; ok && o.Status == payment.StatusPending {
                o.Status = payment.StatusAttempted
                o.ProviderPaymentID = providerPaymentID
        }
        return nil
}

// ApplyPaid mirrors the SQL exactly: non-paid guard, then paid + subscription
// extend-or-insert in the same critical section.
func (s *fakePayStore) ApplyPaid(_ context.Context, orderID, providerPaymentID string) (bool, *payment.Order, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        o, ok := s.byID[orderID]
        if !ok {
                return false, nil, payment.ErrNotFound
        }
        if o.Status == payment.StatusPaid {
                cp := *o
                return false, &cp, nil // absorbing: idempotent no-op
        }
        now := time.Now().UTC()
        o.Status = payment.StatusPaid
        if providerPaymentID != "" {
                o.ProviderPaymentID = providerPaymentID
        }
        o.PaidAt = &now
        o.UpdatedAt = now

        // activateSubscription (payments.go SQL): extend-or-insert, stacking.
        if o.PlanCode != "" && o.PeriodDays > 0 {
                if sub, ok := s.subs[o.TenantID]; ok &&
                        (sub.status == "trialing" || sub.status == "active" || sub.status == "past_due") {
                        base := sub.periodEnd
                        if base.Before(now) {
                                base = now
                        }
                        sub.periodEnd = base.AddDate(0, 0, o.PeriodDays)
                        sub.planCode, sub.status = o.PlanCode, "active"
                        sub.periodDays = o.PeriodDays
                } else {
                        s.subs[o.TenantID] = &fakeSubRow{
                                planCode: o.PlanCode, status: "active",
                                periodEnd: now.AddDate(0, 0, o.PeriodDays), periodDays: o.PeriodDays,
                        }
                }
        }
        cp := *o
        return true, &cp, nil
}

func (s *fakePayStore) ApplyFailed(_ context.Context, orderID, reason string) (bool, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        o, ok := s.byID[orderID]
        if !ok {
                return false, payment.ErrNotFound
        }
        if o.Status != payment.StatusPending && o.Status != payment.StatusAttempted {
                return false, nil // late failure after capture is dropped
        }
        o.Status = payment.StatusFailed
        o.FailureReason = reason
        o.UpdatedAt = time.Now().UTC()
        return true, nil
}

func (s *fakePayStore) ApplyExpired(_ context.Context, orderID string) (bool, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        o, ok := s.byID[orderID]
        if !ok {
                return false, payment.ErrNotFound
        }
        if o.Status != payment.StatusPending && o.Status != payment.StatusAttempted {
                return false, nil
        }
        o.Status = payment.StatusExpired
        o.UpdatedAt = time.Now().UTC()
        return true, nil
}

func (s *fakePayStore) RecordWebhookEvent(_ context.Context, ev *payment.WebhookEvent) (bool, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        if s.events[ev.EventID] {
                return true, nil
        }
        s.events[ev.EventID] = true
        return false, nil
}

func (s *fakePayStore) PlanExists(_ context.Context, planCode string) (bool, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        return s.plans[planCode], nil
}

func (s *fakePayStore) Subscription(_ context.Context, tenantID string) (*payment.SubscriptionState, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        sub, ok := s.subs[tenantID]
        if !ok {
                return &payment.SubscriptionState{TenantID: tenantID, Status: payment.SubNone}, nil
        }
        days := int(time.Until(sub.periodEnd).Hours() / 24)
        if days < 0 {
                days = 0
        }
        end := sub.periodEnd
        return &payment.SubscriptionState{
                TenantID: tenantID, PlanCode: sub.planCode, Status: sub.status,
                CurrentPeriodEnd: &end, DaysRemaining: days,
        }, nil
}

func (s *fakePayStore) ExpireStale(_ context.Context, now time.Time) (int64, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        var n int64
        for _, o := range s.byID {
                if (o.Status == payment.StatusPending || o.Status == payment.StatusAttempted) &&
                        now.After(o.ExpiresAt) {
                        o.Status = payment.StatusExpired
                        o.UpdatedAt = now
                        n++
                }
        }
        return n, nil
}

func (s *fakePayStore) ListOpenOrders(_ context.Context, limit int) ([]*payment.Order, error) {
        return s.listBy(func(o *payment.Order) bool { return !payment.Terminal(o.Status) }, limit)
}

func (s *fakePayStore) ListRecentlyExpired(_ context.Context, within time.Duration, limit int) ([]*payment.Order, error) {
        cutoff := time.Now().Add(-within)
        return s.listBy(func(o *payment.Order) bool {
                return o.Status == payment.StatusExpired && o.UpdatedAt.After(cutoff)
        }, limit)
}

func (s *fakePayStore) listBy(match func(*payment.Order) bool, limit int) ([]*payment.Order, error) {
        s.mu.Lock()
        defer s.mu.Unlock()
        var out []*payment.Order
        for _, o := range s.byID {
                if match(o) {
                        cp := *o
                        out = append(out, &cp)
                }
        }
        if len(out) > limit {
                out = out[:limit]
        }
        return out, nil
}

// --- helpers on the fake ----------------------------------------------------

func (s *fakePayStore) orderCount() int {
        s.mu.Lock()
        defer s.mu.Unlock()
        return len(s.byID)
}

func (s *fakePayStore) sub(tenant string) *fakeSubRow {
        s.mu.Lock()
        defer s.mu.Unlock()
        return s.subs[tenant]
}

// defaultCatalog builds the launch catalog for tests.
func defaultCatalog(t *testing.T) *payment.Catalog {
        t.Helper()
        c, err := payment.NewCatalog(payment.DefaultCatalog())
        if err != nil {
                t.Fatalf("catalog: %v", err)
        }
        return c
}

// echoTamperProvider wraps MockProvider but echoes an amount one paise off —
// the P07 provider-drift probe.
type echoTamperProvider struct{ *payment.MockProvider }

func (e echoTamperProvider) CreateOrder(ctx context.Context, req payment.CreateOrderRequest) (*payment.ProviderOrder, error) {
        po, err := e.MockProvider.CreateOrder(ctx, req)
        if po != nil {
                po.AmountPaise++
        }
        return po, err
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

type payEnv struct {
        svc     *payment.Service
        store   *fakePayStore
        mock    *payment.MockProvider
        mr      *miniredis.Miniredis
}

func newPayEnv(t *testing.T, mutate func(*payment.Config)) *payEnv {
        t.Helper()
        return newPayEnvTenant(t, "ten_buyer", mutate)
}

func newPayEnvTenant(t *testing.T, tenantID string, mutate func(*payment.Config)) *payEnv {
        t.Helper()
        mr := miniredis.RunT(t)
        rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
        t.Cleanup(func() { _ = rdb.Close() })

        store := newFakePayStore()
        mock := payment.NewMockProvider()
        cfg := payment.Config{
                Store: store, Provider: mock,
                Catalog:      defaultCatalog(t),
                OrderTTL:     15 * time.Minute,
                SweepInterval: time.Hour,
                LateCaptureWindow: 24 * time.Hour,
                HistoryLimit:  50,
                CheckoutPerUser: 0, // off unless a journey turns it on
                WebhookPerIP:   0,
                Limiter:        ratelimit.New(rdb, true, observability.NewMetrics()),
        }
        if mutate != nil {
                mutate(&cfg)
        }
        svc := payment.New(cfg)
        return &payEnv{svc: svc, store: store, mock: mock, mr: mr}
}

// checkout is the buyer's first step.
func (be *payEnv) checkout(t *testing.T, tenant, sku, idem string) *payment.Order {
        t.Helper()
        o, err := be.svc.Checkout(context.Background(), tenant, "usr_"+tenant, sku, idem)
        if err != nil {
                t.Fatalf("checkout: %v", err)
        }
        return o
}

// captureAndConfirm runs the honest browser leg: pay at the (mock) provider,
// then POST confirm with the provider-signed handshake.
func (be *payEnv) captureAndConfirm(t *testing.T, tenant string, o *payment.Order) *payment.Order {
        t.Helper()
        payID := be.mock.Capture(o.ProviderOrderID)
        confirmed, err := be.svc.Confirm(context.Background(), tenant, o.ProviderOrderID, payID,
                be.mock.SignCheckout(o.ProviderOrderID, payID))
        if err != nil {
                t.Fatalf("confirm: %v", err)
        }
        return confirmed
}

// webhookEnvelopeJSON builds a razorpay-shaped payment webhook body.
func webhookEnvelopeJSON(event, eventID, payID, orderID, status string, amount int64) []byte {
        env := map[string]any{
                "event": event, "id": eventID, "created_at": time.Now().Unix(),
                "payload": map[string]any{
                        "payment": map[string]any{"entity": map[string]any{
                                "id": payID, "order_id": orderID, "status": status, "amount": amount,
                        }},
                },
        }
        b, _ := json.Marshal(env)
        return b
}

// deliverWebhook posts a correctly-signed webhook from clientIP.
func (be *payEnv) deliverWebhook(t *testing.T, body []byte, ip string) error {
        t.Helper()
        return be.svc.HandleWebhook(context.Background(), body, be.mock.SignWebhook(body), ip)
}

// ---------------------------------------------------------------------------
// the journeys
// ---------------------------------------------------------------------------

// P01-P05: catalog; happy checkout; idempotent replay; key reuse with a
// different SKU; unknown SKU; unconfigured plan.
func TestJourneyP01toP05_CatalogAndCheckoutContract(t *testing.T) {
        be := newPayEnv(t, nil)
        ctx := context.Background()
        tenant := "ten_buyer"

        // P01: catalog — two SKUs, cheapest first, no secrets.
        skus := be.svc.SKUs()
        if len(skus) != 2 || skus[0].ID != "plan_pro_1m" || skus[1].ID != "plan_pro_1y" {
                t.Fatalf("catalog must list monthly then yearly, got %+v", skus)
        }
        if skus[0].AmountPaise != 199900 || skus[0].PeriodDays != 30 || skus[0].PlanCode != "pro" {
                t.Fatalf("monthly SKU snapshot wrong: %+v", skus[0])
        }

        // P02: happy checkout — pending order with the SKU snapshotted.
        o := be.checkout(t, tenant, "plan_pro_1m", "idem-1")
        if o.Status != payment.StatusPending || o.PlanCode != "pro" || o.PeriodDays != 30 ||
                o.AmountPaise != 199900 || o.Provider != payment.ProviderMock {
                t.Fatalf("checkout snapshot wrong: %+v", o)
        }
        if !o.ExpiresAt.After(time.Now()) {
                t.Fatal("order must carry a future TTL")
        }

        // P03: network-retried checkout with the SAME key replays the SAME order.
        again := be.checkout(t, tenant, "plan_pro_1m", "idem-1")
        if again.ID != o.ID {
                t.Fatalf("idempotent replay must return the original order: %s vs %s", again.ID, o.ID)
        }
        if be.store.orderCount() != 1 {
                t.Fatalf("exactly one order row may exist, got %d", be.store.orderCount())
        }

        // P04: same key, DIFFERENT sku → client bug, refused.
        if _, err := be.svc.Checkout(ctx, tenant, "usr_"+tenant, "plan_pro_1y", "idem-1"); err == nil {
                t.Fatal("idempotency-key reuse across SKUs must be refused")
        }
        if be.store.orderCount() != 1 {
                t.Fatalf("refused reuse must not create rows, got %d", be.store.orderCount())
        }

        // P05a: unknown SKU.
        if _, err := be.svc.Checkout(ctx, tenant, "usr_"+tenant, "plan_nonexistent", ""); err == nil {
                t.Fatal("unknown SKU must 404")
        } else if de := domain.AsError(err); de == nil || de.Code != "SKU_NOT_FOUND" {
                t.Fatalf("expected SKU_NOT_FOUND, got %v", err)
        }

        // P05b: catalog SKU whose plan_code has no plans row (operator drift).
        // Needs its own service: the ghost SKU must EXIST in the catalog so the
        // failure comes from the plan guard, not the SKU lookup.
        ghostCat, err := payment.NewCatalog([]payment.PlanSKU{
                {ID: "plan_ghost_1m", Label: "Ghost — ₹999 / month", PlanCode: "ghost", PeriodDays: 30, AmountPaise: 99900},
        })
        if err != nil {
                t.Fatalf("ghost catalog: %v", err)
        }
        ghostSvc := payment.New(payment.Config{Store: newFakePayStore(), Provider: payment.NewMockProvider(),
                Catalog: ghostCat, OrderTTL: time.Minute})
        if _, err := ghostSvc.Checkout(ctx, tenant, "usr_"+tenant, "plan_ghost_1m", ""); err == nil {
                t.Fatal("unconfigured plan must be refused before the provider call")
        } else if de := domain.AsError(err); de == nil || de.Code != "PLAN_NOT_CONFIGURED" {
                t.Fatalf("expected PLAN_NOT_CONFIGURED, got %v", err)
        }
}

// P06: a stuck client retry-storming checkout hits the per-user hourly
// budget — the provider is never fanned out on.
func TestJourneyP06_CheckoutRetryStormBudget(t *testing.T) {
        be := newPayEnv(t, func(c *payment.Config) { c.CheckoutPerUser = 2 })
        ctx := context.Background()
        tenant := "ten_storm"

        for i := 0; i < 2; i++ {
                be.checkout(t, tenant, "plan_pro_1m", fmt.Sprintf("idem-%d", i))
        }
        _, err := be.svc.Checkout(ctx, tenant, "usr_"+tenant, "plan_pro_1m", "idem-3")
        if err == nil {
                t.Fatal("third checkout inside the window must be budget-refused")
        }
        if be.store.orderCount() != 2 {
                t.Fatalf("budget must bound order creation, got %d rows", be.store.orderCount())
        }
}

// P07: the provider echoing a different amount than requested is a loud
// failure — nothing created, nothing activatable.
func TestJourneyP07_ProviderAmountEchoTamper(t *testing.T) {
        store := newFakePayStore()
        mock := payment.NewMockProvider()
        svc := payment.New(payment.Config{
                Store: store, Provider: echoTamperProvider{mock},
                Catalog: defaultCatalog(t), OrderTTL: time.Minute,
        })
        if _, err := svc.Checkout(context.Background(), "ten_echo", "usr_e", "plan_pro_1m", ""); err == nil {
                t.Fatal("tampered provider echo must fail the checkout")
        }
        if store.orderCount() != 0 {
                t.Fatalf("no local row may survive a tampered echo, got %d", store.orderCount())
        }
}

// P08: the full honest buyer path — checkout, pay, confirm — activates the
// plan exactly once with the snapshotted period.
func TestJourneyP08_ConfirmHappyPathActivatesPlan(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_happy"

        o := be.checkout(t, tenant, "plan_pro_1m", "idem-h")
        confirmed := be.captureAndConfirm(t, tenant, o)

        if confirmed.Status != payment.StatusPaid || confirmed.PaidAt == nil {
                t.Fatalf("confirm must capture the order: %+v", confirmed)
        }
        sub := be.store.sub(tenant)
        if sub == nil || sub.planCode != "pro" || sub.status != "active" {
                t.Fatalf("plan must activate with the order, got %+v", sub)
        }
        want := time.Now().AddDate(0, 0, 30)
        if sub.periodEnd.Before(want.Add(-2 * time.Minute)) || sub.periodEnd.After(want.Add(2 * time.Minute)) {
                t.Fatalf("period must be ~30d from now, got %v (want ~%v)", sub.periodEnd, want)
        }
}

// P09: confirming ANOTHER tenant's order is indistinguishable from an
// unknown order — no cross-tenant existence oracle.
func TestJourneyP09_CrossTenantConfirmIs404(t *testing.T) {
        be := newPayEnv(t, nil)
        o := be.checkout(t, "ten_victim", "plan_pro_1m", "idem-v")
        payID := be.mock.Capture(o.ProviderOrderID)

        _, err := be.svc.Confirm(context.Background(), "ten_attacker", o.ProviderOrderID, payID,
                be.mock.SignCheckout(o.ProviderOrderID, payID))
        if err == nil {
                t.Fatal("foreign order confirm must fail")
        }
        if de := domain.AsError(err); de == nil || de.HTTP != 404 {
                t.Fatalf("cross-tenant confirm must be a 404 (same as unknown), got %v", err)
        }
        if o2, _ := be.store.OrderByID(context.Background(), o.ID); o2.Status != payment.StatusPending {
                t.Fatalf("the victim's order must be untouched, got %s", o2.Status)
        }
}

// P10: the webhook wins the race; the user's later confirm is a settled
// success — and the plan activated EXACTLY once.
func TestJourneyP10_WebhookWonRaceThenConfirmIsSettled(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_race"
        o := be.checkout(t, tenant, "plan_pro_1m", "idem-r")

        payID := be.mock.Capture(o.ProviderOrderID)
        body := webhookEnvelopeJSON("payment.captured", "evt_race_1", payID, o.ProviderOrderID, "captured", o.AmountPaise)
        if err := be.deliverWebhook(t, body, "10.0.0.9"); err != nil {
                t.Fatalf("webhook: %v", err)
        }

        // The browser comes home late and confirms the SAME payment.
        confirmed, err := be.svc.Confirm(context.Background(), tenant, o.ProviderOrderID, payID,
                be.mock.SignCheckout(o.ProviderOrderID, payID))
        if err != nil {
                t.Fatalf("confirm after webhook must be settled success: %v", err)
        }
        if confirmed.Status != payment.StatusPaid {
                t.Fatalf("order must be paid, got %s", confirmed.Status)
        }
        // Exactly once: the period is now+30d, NOT now+60d.
        sub := be.store.sub(tenant)
        want := time.Now().AddDate(0, 0, 30)
        if sub.periodEnd.Before(want.Add(-2*time.Minute)) || sub.periodEnd.After(want.Add(2*time.Minute)) {
                t.Fatalf("double-apply detected: period end %v, want ~%v", sub.periodEnd, want)
        }
}

// P11 + P12: forged checkout signature; and a payment that belongs to a
// different order.
func TestJourneyP11andP12_ConfirmForgeryAndMismatch(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_forge"
        o1 := be.checkout(t, tenant, "plan_pro_1m", "idem-f1")
        pay1 := be.mock.Capture(o1.ProviderOrderID)

        // P11: attacker-signed handshake.
        if _, err := be.svc.Confirm(context.Background(), tenant, o1.ProviderOrderID, pay1, "00deadbeef"); err == nil {
                t.Fatal("forged signature must be refused")
        } else if de := domain.AsError(err); de == nil || de.Code != "PAYMENT_SIGNATURE_INVALID" {
                t.Fatalf("expected PAYMENT_SIGNATURE_INVALID, got %v", err)
        }

        // P12: order B confirmed with order A's payment (valid signature over
        // the WRONG pair) — the provider fetch exposes the mismatch.
        o2 := be.checkout(t, tenant, "plan_pro_1m", "idem-f2")
        if _, err := be.svc.Confirm(context.Background(), tenant, o2.ProviderOrderID, pay1,
                be.mock.SignCheckout(o2.ProviderOrderID, pay1)); err == nil {
                t.Fatal("cross-order payment must be refused")
        }
        if o2r, _ := be.store.OrderByID(context.Background(), o2.ID); o2r.Status != payment.StatusPending {
                t.Fatalf("order 2 must stay pending, got %s", o2r.Status)
        }
}

// P13 + P14: the webhook happy path; duplicates converge without
// double-applying.
func TestJourneyP13andP14_WebhookHappyAndDuplicateConverge(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_wh"
        o := be.checkout(t, tenant, "plan_pro_1m", "idem-w")

        payID := be.mock.Capture(o.ProviderOrderID)
        body := webhookEnvelopeJSON("payment.captured", "evt_dup_1", payID, o.ProviderOrderID, "captured", o.AmountPaise)
        if err := be.deliverWebhook(t, body, "10.0.0.1"); err != nil {
                t.Fatalf("webhook happy: %v", err)
        }
        if o1, _ := be.store.OrderByID(context.Background(), o.ID); o1.Status != payment.StatusPaid {
                t.Fatalf("webhook must apply the payment, got %s", o1.Status)
        }

        // The provider retries the SAME event id: acknowledged, converged (no
        // second activation), never an error.
        if err := be.deliverWebhook(t, body, "10.0.0.1"); err != nil {
                t.Fatalf("duplicate webhook must be acked: %v", err)
        }
        sub := be.store.sub(tenant)
        want := time.Now().AddDate(0, 0, 30)
        if sub.periodEnd.Before(want.Add(-2*time.Minute)) || sub.periodEnd.After(want.Add(2*time.Minute)) {
                t.Fatalf("duplicate must not double-extend: end %v, want ~%v", sub.periodEnd, want)
        }
}

// P15: out-of-order delivery — payment.failed arriving AFTER
// payment.captured is dropped: paid is absorbing.
func TestJourneyP15_OutOfOrderFailedAfterCaptured(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_ooo"
        o := be.checkout(t, tenant, "plan_pro_1m", "idem-o")

        payID := be.mock.Capture(o.ProviderOrderID)
        capBody := webhookEnvelopeJSON("payment.captured", "evt_ooo_cap", payID, o.ProviderOrderID, "captured", o.AmountPaise)
        if err := be.deliverWebhook(t, capBody, "10.0.0.2"); err != nil {
                t.Fatalf("captured: %v", err)
        }
        failBody := webhookEnvelopeJSON("payment.failed", "evt_ooo_fail", payID, o.ProviderOrderID, "failed", o.AmountPaise)
        if err := be.deliverWebhook(t, failBody, "10.0.0.2"); err != nil {
                t.Fatalf("late failure must be acknowledged, not error: %v", err)
        }
        if o1, _ := be.store.OrderByID(context.Background(), o.ID); o1.Status != payment.StatusPaid {
                t.Fatalf("paid is absorbing — late failure must not regress, got %s", o1.Status)
        }
        if be.store.sub(tenant) == nil || be.store.sub(tenant).planCode != "pro" {
                t.Fatal("the activation must survive the late failure")
        }
}

// P16: hostile webhooks — forged signature, amount tamper (alarm + hold),
// and an event for an order that is not ours.
func TestJourneyP16_HostileWebhooks(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_hostile"
        o := be.checkout(t, tenant, "plan_pro_1m", "idem-h")
        payID := be.mock.Capture(o.ProviderOrderID)

        // Forged signature: fail closed before parse.
        body := webhookEnvelopeJSON("payment.captured", "evt_forge", payID, o.ProviderOrderID, "captured", o.AmountPaise)
        if err := be.svc.HandleWebhook(context.Background(), body, "0" + strings.Repeat("0", 63), "9.9.9.9"); err == nil {
                t.Fatal("forged webhook signature must be rejected")
        }

        // Correctly signed but WRONG amount: tamper alarm, order untouched.
        tampered := webhookEnvelopeJSON("payment.captured", "evt_amount", payID, o.ProviderOrderID, "captured", 100)
        if err := be.deliverWebhook(t, tampered, "9.9.9.9"); err == nil {
                t.Fatal("amount mismatch must be an alarm")
        } else if de := domain.AsError(err); de == nil || de.Code != "PAYMENT_AMOUNT_MISMATCH" {
                t.Fatalf("expected PAYMENT_AMOUNT_MISMATCH, got %v", err)
        }
        if o1, _ := be.store.OrderByID(context.Background(), o.ID); o1.Status != payment.StatusPending {
                t.Fatalf("tampered-amount order must stay pending, got %s", o1.Status)
        }

        // Unknown order (another product's webhook URL): recorded, acked, no state.
        orphan := webhookEnvelopeJSON("payment.captured", "evt_orphan", "pay_x", "order_unknown", "captured", 199900)
        if err := be.deliverWebhook(t, orphan, "9.9.9.9"); err != nil {
                t.Fatalf("orphan event must be acknowledged: %v", err)
        }
        if o1, _ := be.store.OrderByID(context.Background(), o.ID); o1.Status != payment.StatusPending {
                t.Fatalf("orphan must not touch our order, got %s", o1.Status)
        }
}

// P17: refund events are recorded and acknowledged — but the entitlement is
// NOT auto-reversed (v2 manual reconciliation, made visible).
func TestJourneyP17_RefundRecordedWithoutReversal(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_refund"
        o := be.checkout(t, tenant, "plan_pro_1m", "idem-rf")
        payID := be.mock.Capture(o.ProviderOrderID)
        be.captureAndConfirm(t, tenant, o)

        refund := webhookEnvelopeJSON("refund.processed", "evt_refund_1", payID, o.ProviderOrderID, "refunded", o.AmountPaise)
        if err := be.deliverWebhook(t, refund, "10.0.0.3"); err != nil {
                t.Fatalf("refund event must be recorded + acked: %v", err)
        }
        // Entitlement intentionally NOT reversed (documented v2 gap).
        if sub := be.store.sub(tenant); sub == nil || sub.planCode != "pro" {
                t.Fatalf("refund must not silently reverse entitlement (manual v2 path), got %+v", sub)
        }
        // The event is recorded for the manual reconciliation trail.
        be.store.mu.Lock()
        recorded := be.store.events["evt_refund_1"]
        be.store.mu.Unlock()
        if !recorded {
                t.Fatal("refund event must be recorded")
        }
}

// P18: a webhook flood from one IP hits the per-IP minute budget before the
// HMAC+DB work.
func TestJourneyP18_WebhookFloodBudget(t *testing.T) {
        be := newPayEnv(t, func(c *payment.Config) { c.WebhookPerIP = 2 })
        garbage := webhookEnvelopeJSON("payment.captured", "evt_flood", "pay_f", "order_f", "captured", 1)
        for i := 0; i < 2; i++ {
                if err := be.svc.HandleWebhook(context.Background(), garbage, "0bad", "8.8.8.8"); err == nil {
                        t.Fatal("garbage signature must be rejected")
                }
        }
        // Budget consumed by the two attempts: the third is refused on budget.
        err := be.svc.HandleWebhook(context.Background(), garbage, "0bad", "8.8.8.8")
        if err == nil {
                t.Fatal("flood must hit the budget")
        }
        if de := domain.AsError(err); de == nil || de.HTTP != 429 {
                t.Fatalf("budget refusal must be 429, got %v", err)
        }
}

// P19: the abandoned checkout — order expires via the real sweeper loop;
// nothing is activated.
func TestJourneyP19_AbandonedCheckoutExpires(t *testing.T) {
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()

        be := newPayEnv(t, func(c *payment.Config) {
                c.OrderTTL, c.SweepInterval = 60*time.Millisecond, 20*time.Millisecond
        })
        be.svc.StartSweeper(ctx) // the real background loop

        o := be.checkout(t, "ten_ghost", "plan_pro_1m", "idem-x")
        deadline := time.Now().Add(2 * time.Second)
        for time.Now().Before(deadline) {
                if cur, _ := be.store.OrderByID(ctx, o.ID); cur != nil && cur.Status == payment.StatusExpired {
                        // Expired, and nothing was ever activated.
                        if be.store.sub("ten_ghost") != nil {
                                t.Fatal("an unpaid order must never activate a plan")
                        }
                        return
                }
                time.Sleep(10 * time.Millisecond)
        }
        t.Fatal("order never expired — sweeper did not run")
}

// P21: the user paid, but BOTH the webhook and the browser return were lost
// until AFTER our TTL expired the order — the late capture still wins.
func TestJourneyP21_LateCaptureOnExpiredOrder(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_late"
        o := be.checkout(t, tenant, "plan_pro_1m", "idem-late")

        // TTL passes without capture: the sweeper expires it.
        if _, err := be.store.ExpireStale(context.Background(), time.Now().Add(time.Hour)); err != nil {
                t.Fatalf("expire: %v", err)
        }
        if o1, _ := be.store.OrderByID(context.Background(), o.ID); o1.Status != payment.StatusExpired {
                t.Fatalf("precondition: order must be expired, got %s", o1.Status)
        }

        // The settlement lands late (a valid captured webhook for the expired
        // order) — money is real, it must win.
        payID := be.mock.Capture(o.ProviderOrderID)
        body := webhookEnvelopeJSON("payment.captured", "evt_late_1", payID, o.ProviderOrderID, "captured", o.AmountPaise)
        if err := be.deliverWebhook(t, body, "10.0.0.4"); err != nil {
                t.Fatalf("late capture must be applied: %v", err)
        }
        if o1, _ := be.store.OrderByID(context.Background(), o.ID); o1.Status != payment.StatusPaid {
                t.Fatalf("late capture must beat expiry, got %s", o1.Status)
        }
        if sub := be.store.sub(tenant); sub == nil || sub.planCode != "pro" {
                t.Fatalf("late capture must activate the plan, got %+v", sub)
        }
}

// P22: an honest failure — the order ends failed (terminal), and the user
// can immediately check out again and succeed.
func TestJourneyP22_FailedPaymentThenCleanRetry(t *testing.T) {
        be := newPayEnv(t, nil)
        tenant := "ten_retry"
        o1 := be.checkout(t, tenant, "plan_pro_1m", "idem-try1")

        be.mock.Fail(o1.ProviderOrderID)
        failBody := webhookEnvelopeJSON("payment.failed", "evt_fail_1", "pay_fail_1", o1.ProviderOrderID, "failed", o1.AmountPaise)
        if err := be.deliverWebhook(t, failBody, "10.0.0.5"); err != nil {
                t.Fatalf("failure webhook: %v", err)
        }
        if o1r, _ := be.store.OrderByID(context.Background(), o1.ID); o1r.Status != payment.StatusFailed {
                t.Fatalf("order must be failed, got %s", o1r.Status)
        }

        // The retry: fresh checkout, paid this time.
        o2 := be.checkout(t, tenant, "plan_pro_1m", "idem-try2")
        be.captureAndConfirm(t, tenant, o2)
        if sub := be.store.sub(tenant); sub == nil || sub.planCode != "pro" {
                t.Fatalf("retry must activate the plan, got %+v", sub)
        }
}
