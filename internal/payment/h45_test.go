// h45_test.go — audit H4 (webhook duplicate convergence) and H5 (late-capture
// recovery for expired orders) regression scenarios.
package payment_test

import (
	"context"
	"testing"
	"time"

	"github.com/mash-cloud/mash-api/internal/payment"
)

// TestWebhook_DuplicateConvergesAfterCrashWindow is the H4 regression: the
// first delivery recorded the event but died BEFORE applying it (rolling
// deploy / OOM — exactly the millisecond window the audit describes). The
// provider's retry arrives with the SAME event id: the old dedupe swallowed
// it (200, never applied — payment lost forever); the fixed handler converges
// the order to paid.
func TestWebhook_DuplicateConvergesAfterCrashWindow(t *testing.T) {
	f := newFixture(t)
	sku := firstSKU(t)
	o, _ := f.svc.Checkout(context.Background(), tenantA, userA, sku.ID, "")
	payID := f.provider.Capture(o.ProviderOrderID)

	body := payment.BuildWebhookBody("evt_crash", payment.EventPaymentCaptured, o.ProviderOrderID, payID, sku.AmountPaise)

	// Simulate the crash window: the event was RECORDED but the apply never
	// ran (that is the exact state a killed process leaves behind).
	ev := &payment.WebhookEvent{
		EventID: "evt_crash", Type: payment.EventPaymentCaptured,
		Provider: f.provider.Name(), ProviderOrderID: o.ProviderOrderID,
		ProviderPaymentID: payID, AmountPaise: sku.AmountPaise,
	}
	if dup, err := f.store.RecordWebhookEvent(context.Background(), ev); err != nil || dup {
		t.Fatalf("seed record: dup=%v err=%v", dup, err)
	}

	// The provider retries (same event id).
	if err := f.svc.HandleWebhook(context.Background(), body, f.provider.SignWebhook(body), "10.0.0.1"); err != nil {
		t.Fatal(err)
	}
	got, _ := f.store.OrderByID(context.Background(), o.ID)
	if got.Status != payment.StatusPaid {
		t.Fatalf("H4 regression: retried event was swallowed — status %s, want paid", got.Status)
	}
	st, _ := f.store.Subscription(context.Background(), tenantA)
	if st.Status != payment.SubActive {
		t.Fatalf("plan not activated by converged duplicate: %+v", st)
	}
}

// TestLateCapture_ExpiredOrderStillConverges is the H5 regression: money left
// the bank, the webhook never arrived, the browser died — the order expired.
// A polling client (GetOrder read-through) must still converge it to paid,
// because the provider is the authority and paid beats expired.
func TestLateCapture_ExpiredOrderStillConverges(t *testing.T) {
	f := newFixture(t)
	sku := firstSKU(t)
	o, _ := f.svc.Checkout(context.Background(), tenantA, userA, sku.ID, "")

	// The order expires locally (TTL passes, no capture news arrived).
	expired := time.Now().UTC().Add(-2 * time.Minute)
	f.store.ExpireOrderAtForTest(o.ID, expired)
	// The settlement lands late (capture recorded provider-side).
	_ = f.provider.Capture(o.ProviderOrderID)

	got, err := f.svc.GetOrder(context.Background(), tenantA, o.ID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if got.Status != payment.StatusPaid {
		t.Fatalf("H5 regression: late capture on an expired order — status %s, want paid", got.Status)
	}
	st, _ := f.store.Subscription(context.Background(), tenantA)
	if st.Status != payment.SubActive || st.PlanCode != sku.PlanCode {
		t.Fatalf("late capture did not activate the plan: %+v", st)
	}
}

// TestLateCapture_WindowIsBounded: an order that expired LONG ago (beyond the
// late-capture window) must not fan out to the provider on reads.
func TestLateCapture_WindowIsBounded(t *testing.T) {
	f := newFixture(t)
	sku := firstSKU(t)
	o, _ := f.svc.Checkout(context.Background(), tenantA, userA, sku.ID, "")

	f.store.ExpireOrderAtForTest(o.ID, time.Now().UTC().Add(-72*time.Hour))
	f.provider.Capture(o.ProviderOrderID)
	f.provider.ResetFetchCount()

	got, err := f.svc.GetOrder(context.Background(), tenantA, o.ID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if got.Status != payment.StatusExpired {
		t.Fatalf("orders past the late-capture window must stay expired, got %s", got.Status)
	}
	if n := f.provider.FetchCount(); n != 0 {
		t.Fatalf("read-through must not fan out beyond the window: %d provider fetches", n)
	}
}
