package config

// Audit M12/M13 regression tests: boot-time payment configuration guards.

import (
	"strings"
	"testing"
)

func validBaseEnv() map[string]string {
	return map[string]string{
		"NEXAU_DATABASE_URL":       "postgres://x@127.0.0.1:5/p?sslmode=disable",
		"NEXAU_REDIS_URL":          "redis://127.0.0.1:6",
		"NEXAU_OPENROUTER_API_KEY": "sk-or-v1-testkey",
		"NEXAU_AUTH_HS256_SECRET":  "e3b1e66a13bf952acc470716439ce642b06df2995ec510f9e5830ebf005c9be9",
	}
}

func loadWith(t *testing.T, extra map[string]string) error {
	t.Helper()
	for k, v := range validBaseEnv() {
		t.Setenv(k, v)
	}
	for k, v := range extra {
		t.Setenv(k, v)
	}
	_, err := Load()
	return err
}

// TestMockProviderRequiresExplicitOptIn (M13): provider=mock without the dev
// flag must fail boot — mock secrets are committed to the repo, so a stray
// staging manifest in prod equals webhook forgery = free Pro.
func TestMockProviderRequiresExplicitOptIn(t *testing.T) {
	err := loadWith(t, map[string]string{"NEXAU_PAYMENT_PROVIDER": "mock"})
	if err == nil || !strings.Contains(err.Error(), "NEXAU_PAYMENT_ALLOW_MOCK") {
		t.Fatalf("mock without opt-in must fail boot, got %v", err)
	}
	if err := loadWith(t, map[string]string{
		"NEXAU_PAYMENT_PROVIDER": "mock", "NEXAU_PAYMENT_ALLOW_MOCK": "true",
	}); err != nil {
		t.Fatalf("mock with explicit opt-in must boot: %v", err)
	}
}

// TestPhonepeRequiresCallbackURL (M12): provider=phonepe without the callback
// URL must fail boot — half of payment recovery would be silently disabled.
func TestPhonepeRequiresCallbackURL(t *testing.T) {
	err := loadWith(t, map[string]string{
		"NEXAU_PAYMENT_PROVIDER":     "phonepe",
		"NEXAU_PHONEPE_MERCHANT_ID":  "m",
		"NEXAU_PHONEPE_SALT_KEY":     "k",
		"NEXAU_PHONEPE_SALT_INDEX":   "1",
		"NEXAU_PHONEPE_REDIRECT_URL": "https://pay.example.test/return",
	})
	if err == nil || !strings.Contains(err.Error(), "NEXAU_PHONEPE_CALLBACK_URL") {
		t.Fatalf("phonepe without callback URL must fail boot, got %v", err)
	}
	if err := loadWith(t, map[string]string{
		"NEXAU_PAYMENT_PROVIDER":     "phonepe",
		"NEXAU_PHONEPE_MERCHANT_ID":  "m",
		"NEXAU_PHONEPE_SALT_KEY":     "k",
		"NEXAU_PHONEPE_SALT_INDEX":   "1",
		"NEXAU_PHONEPE_REDIRECT_URL": "https://pay.example.test/return",
		"NEXAU_PHONEPE_CALLBACK_URL": "https://api.example.test/v1/payments/webhook",
	}); err != nil {
		t.Fatalf("phonepe with callback URL must boot: %v", err)
	}
}
