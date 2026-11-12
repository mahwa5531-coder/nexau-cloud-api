// h6_test.go — audit H6 regression: confirm/status read-through fan-out to
// the metered provider API is bounded per tenant.
package payment_test

import (
	"context"
	"testing"
	"time"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/payment"
)

// TestReconcileFanOutIsBounded: a polling loop on order status must stop
// hitting the provider once the per-tenant budget is spent (the stored order
// is served); the provider fetch counter proves it.
func TestReconcileFanOutIsBounded(t *testing.T) {
	f := newFixture(t)
	f.svc = payment.New(payment.Config{
		Store: f.store, Provider: f.provider, Catalog: mustCatalog(t),
		OrderTTL: 15 * time.Minute, SweepInterval: time.Hour,
		LateCaptureWindow: 24 * time.Hour, HistoryLimit: 50,
		CheckoutPerUser: 100, WebhookPerIP: 1000,
		ReconcilePerTenant: 3, // tiny budget for the test
		Limiter:            &budgetLimiter{},
	})
	sku := firstSKU(t)
	o, _ := f.svc.Checkout(context.Background(), tenantA, userA, sku.ID, "")

	f.provider.ResetFetchCount()
	for i := 0; i < 10; i++ {
		if _, err := f.svc.GetOrder(context.Background(), tenantA, o.ID); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}
	if n := f.provider.FetchCount(); n > 3 {
		t.Fatalf("H6 regression: %d provider fetches for 10 polls (budget 3)", n)
	}
}

// TestConfirmOverBudget: confirm beyond the budget answers a 429, not a
// provider call.
func TestConfirmOverBudget(t *testing.T) {
	f := newFixture(t)
	f.svc = payment.New(payment.Config{
		Store: f.store, Provider: f.provider, Catalog: mustCatalog(t),
		OrderTTL: 15 * time.Minute, SweepInterval: time.Hour,
		LateCaptureWindow: 24 * time.Hour, HistoryLimit: 50,
		CheckoutPerUser: 100, WebhookPerIP: 1000,
		ReconcilePerTenant: 1,
		Limiter:            &budgetLimiter{},
	})
	sku := firstSKU(t)
	o, _ := f.svc.Checkout(context.Background(), tenantA, userA, sku.ID, "")
	payID := f.provider.Capture(o.ProviderOrderID)

	// First confirm succeeds (budget 1).
	if _, err := f.svc.Confirm(context.Background(), tenantA, o.ProviderOrderID, payID,
		f.provider.SignCheckout(o.ProviderOrderID, payID)); err != nil {
		t.Fatalf("first confirm: %v", err)
	}
	// A second confirm on a fresh order exceeds the budget → 429.
	o2, _ := f.svc.Checkout(context.Background(), tenantA, userA, sku.ID, "idem2")
	payID2 := f.provider.Capture(o2.ProviderOrderID)
	_, err := f.svc.Confirm(context.Background(), tenantA, o2.ProviderOrderID, payID2,
		f.provider.SignCheckout(o2.ProviderOrderID, payID2))
	if err == nil {
		t.Fatal("over-budget confirm must fail")
	}
	if de := domain.AsError(err); de == nil || de.HTTP != 429 {
		t.Fatalf("want 429 RECONCILE_RATE_LIMITED, got %v", err)
	}
}
