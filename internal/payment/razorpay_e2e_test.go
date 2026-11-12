// razorpay_e2e_test.go — the Razorpay provider gets the same clean E2E
// treatment as PhonePe and Stripe: the REAL razorpay.Client wired into the
// full payment Service against a wire-compatible fake Razorpay server.
// Every scenario is a production failure mode:
//
//	webhook-first ordering · forged checkout handshake · payment.failed ·
//	missed-everything recovered by the status-query reconcile · signed
//	amount-tamper alarm · unknown-payment handshake
package payment_test

import (
	"context"
	"testing"
	"time"

	"github.com/mash-cloud/mash-api/internal/domain"
	"github.com/mash-cloud/mash-api/internal/payment"
	"github.com/mash-cloud/mash-api/internal/payment/razorpay"
)

func newRazorpayEnv(t *testing.T) (*fakeRZP, *razorpay.Client, *payment.Service, *fakeStore) {
	t.Helper()
	fake := newFakeRazorpayServer(t)
	client := razorpay.New(razorpay.Config{
		KeyID: fake.keyID, KeySecret: fake.secret, WebhookSecret: "wh_fake",
		BaseURL: fake.url,
	})
	store := newFakeStore()
	svc := payment.New(payment.Config{
		Store: store, Provider: client, Catalog: mustCatalog(t),
		OrderTTL: time.Minute, SweepInterval: time.Hour, HistoryLimit: 10,
	})
	return fake, client, svc, store
}

// TestRazorpayE2E_WebhookFirstThenConfirmIdempotent: the webhook wins the
// race (the normal production ordering); a LATER client confirm with a fully
// valid handshake must settle, not conflict, and must not double-activate.
func TestRazorpayE2E_WebhookFirstThenConfirmIdempotent(t *testing.T) {
	fake, client, svc, store := newRazorpayEnv(t)
	ctx := context.Background()

	sku := firstSKU(t)
	o, err := svc.Checkout(ctx, tenantA, userA, sku.ID, "")
	if err != nil {
		t.Fatal(err)
	}

	// Payment completes at the provider; ONLY the webhook arrives first.
	payID := fake.captureOrder(o.ProviderOrderID, sku.AmountPaise)
	body := payment.BuildWebhookBody("evt_rzp_wf_1", payment.EventPaymentCaptured, o.ProviderOrderID, payID, sku.AmountPaise)
	if err := svc.HandleWebhook(ctx, body, client.SignWebhookBody(body), "127.0.0.1"); err != nil {
		t.Fatalf("webhook-first delivery rejected: %v", err)
	}

	// The user's browser finishes checkout afterwards: confirm must return
	// the paid order (settled success), not an error, not a second credit.
	sig := client.SignCheckoutPair(o.ProviderOrderID, payID)
	confirmed, err := svc.Confirm(ctx, tenantA, o.ProviderOrderID, payID, sig)
	if err != nil {
		t.Fatalf("late confirm after webhook: %v", err)
	}
	if confirmed.Status != payment.StatusPaid {
		t.Fatalf("confirm status = %s, want paid", confirmed.Status)
	}

	st, _ := store.Subscription(ctx, tenantA)
	if st.Status != payment.SubActive || st.PlanCode != sku.PlanCode {
		t.Fatalf("subscription = %+v, want exactly one activation of %s", st, sku.PlanCode)
	}
	if st.DaysRemaining > sku.PeriodDays {
		t.Fatalf("days = %d: double activation (plan period %d)", st.DaysRemaining, sku.PeriodDays)
	}
}

// TestRazorpayE2E_ForgedCheckoutHandshakeRejected: a handshake that fails
// HMAC verification must never reach the provider or the store — no
// activation, order stays open for the real payment.
func TestRazorpayE2E_ForgedCheckoutHandshakeRejected(t *testing.T) {
	fake, client, svc, store := newRazorpayEnv(t)
	ctx := context.Background()

	sku := firstSKU(t)
	o, err := svc.Checkout(ctx, tenantA, userA, sku.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	payID := fake.captureOrder(o.ProviderOrderID, sku.AmountPaise)

	if _, err := svc.Confirm(ctx, tenantA, o.ProviderOrderID, payID, " forged-signature "); err == nil {
		t.Fatal("forged checkout handshake accepted")
	} else if de := domain.AsError(err); de == nil || de.Code != "PAYMENT_SIGNATURE_INVALID" {
		t.Fatalf("forged handshake error shape: %v", err)
	}
	// Order must still be payable: a valid handshake right after succeeds.
	sig := client.SignCheckoutPair(o.ProviderOrderID, payID)
	if _, err := svc.Confirm(ctx, tenantA, o.ProviderOrderID, payID, sig); err != nil {
		t.Fatalf("valid handshake after forgery rejected: %v", err)
	}
	st, _ := store.Subscription(ctx, tenantA)
	if st.Status != payment.SubActive {
		t.Fatalf("subscription = %+v after valid confirm", st)
	}
}

// TestRazorpayE2E_FailurePathNoActivation: payment.failed is terminal for
// the order; no entitlement mints; a later captured webhook for the SAME
// order still converges (failed is superseded by real money).
func TestRazorpayE2E_FailurePathNoActivation(t *testing.T) {
	fake, client, svc, store := newRazorpayEnv(t)
	ctx := context.Background()

	sku := firstSKU(t)
	o, err := svc.Checkout(ctx, tenantA, userA, sku.ID, "")
	if err != nil {
		t.Fatal(err)
	}

	body := payment.BuildWebhookBody("evt_rzp_f_1", payment.EventPaymentFailed, o.ProviderOrderID, "pay_fail_1", sku.AmountPaise)
	if err := svc.HandleWebhook(ctx, body, client.SignWebhookBody(body), "127.0.0.1"); err != nil {
		t.Fatalf("payment.failed rejected: %v", err)
	}
	got, err := svc.GetOrder(ctx, tenantA, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payment.StatusFailed {
		t.Fatalf("status after payment.failed = %s", got.Status)
	}
	st, _ := store.Subscription(ctx, tenantA)
	if st.Status == payment.SubActive {
		t.Fatalf("subscription activated from a FAILED payment: %+v", st)
	}

	// Real money beats the earlier failure: the capture webhook converges.
	payID := fake.captureOrder(o.ProviderOrderID, sku.AmountPaise)
	body = payment.BuildWebhookBody("evt_rzp_f_2", payment.EventPaymentCaptured, o.ProviderOrderID, payID, sku.AmountPaise)
	if err := svc.HandleWebhook(ctx, body, client.SignWebhookBody(body), "127.0.0.1"); err != nil {
		t.Fatalf("capture after failure rejected: %v", err)
	}
	st, _ = store.Subscription(ctx, tenantA)
	if st.Status != payment.SubActive {
		t.Fatalf("subscription after late capture = %+v", st)
	}
}

// TestRazorpayE2E_MissedEverythingReconcileRecovers: webhook lost AND the
// user closed the browser before confirm — the provider-side truth is
// fetched by the order-status query (read-through reconcile) and activates
// the plan without any user action.
func TestRazorpayE2E_MissedEverythingReconcileRecovers(t *testing.T) {
	fake, _, svc, store := newRazorpayEnv(t)
	ctx := context.Background()

	sku := firstSKU(t)
	o, err := svc.Checkout(ctx, tenantA, userA, sku.ID, "")
	if err != nil {
		t.Fatal(err)
	}

	// Money moved at the provider; NO webhook, NO confirm.
	fake.captureOrder(o.ProviderOrderID, sku.AmountPaise)

	got, err := svc.GetOrder(ctx, tenantA, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != payment.StatusPaid {
		t.Fatalf("reconciled status = %s, want paid (provider truth)", got.Status)
	}
	st, _ := store.Subscription(ctx, tenantA)
	if st.Status != payment.SubActive || st.PlanCode != sku.PlanCode {
		t.Fatalf("subscription after reconcile = %+v", st)
	}
}

// TestRazorpayE2E_WebhookAmountMismatchTamperAlarm: a correctly-SIGNED event
// carrying the wrong amount for its order is a tamper alarm — recorded,
// rejected for application, order untouched.
func TestRazorpayE2E_WebhookAmountMismatchTamperAlarm(t *testing.T) {
	fake, client, svc, store := newRazorpayEnv(t)
	ctx := context.Background()

	sku := firstSKU(t)
	o, err := svc.Checkout(ctx, tenantA, userA, sku.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	payID := fake.captureOrder(o.ProviderOrderID, sku.AmountPaise)

	// Signed with the RIGHT secret but carrying ONE PAISE.
	body := payment.BuildWebhookBody("evt_rzp_tamper", payment.EventPaymentCaptured, o.ProviderOrderID, payID, 1)
	if err := svc.HandleWebhook(ctx, body, client.SignWebhookBody(body), "127.0.0.1"); err == nil {
		t.Fatal("amount-mismatch webhook applied")
	} else if de := domain.AsError(err); de == nil || de.Code != "PAYMENT_AMOUNT_MISMATCH" {
		t.Fatalf("unexpected error shape: %v", err)
	}

	// The ORDER row must be untouched by the mismatched event — inspected
	// through the STORE (GetOrder would read-through reconcile against the
	// real provider-side capture, which is exactly its job).
	got, err := store.OrderByID(ctx, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status == payment.StatusPaid {
		t.Fatal("order paid from a 1-paise signed event")
	}
	st, _ := store.Subscription(ctx, tenantA)
	if st.Status == payment.SubActive {
		t.Fatal("subscription activated from a mismatched amount")
	}
}

// TestRazorpayE2E_UnknownPaymentHandshake: a syntactically valid handshake
// naming a payment the provider never minted fails the three-legged check at
// the provider fetch — the order stays open, nothing activates.
func TestRazorpayE2E_UnknownPaymentHandshake(t *testing.T) {
	fake, client, svc, store := newRazorpayEnv(t)
	ctx := context.Background()

	sku := firstSKU(t)
	o, err := svc.Checkout(ctx, tenantA, userA, sku.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	fake.captureOrder(o.ProviderOrderID, sku.AmountPaise)

	ghost := "pay_NEVER_MINTED"
	sig := client.SignCheckoutPair(o.ProviderOrderID, ghost)
	if _, err := svc.Confirm(ctx, tenantA, o.ProviderOrderID, ghost, sig); err == nil {
		t.Fatal("unknown payment accepted through confirm")
	}
	st, _ := store.Subscription(ctx, tenantA)
	if st.Status == payment.SubActive {
		t.Fatal("subscription activated from an unknown payment")
	}
	// The order remains resolvable and payable via the reconcile path.
	got, err := svc.GetOrder(ctx, tenantA, o.ID)
	if err != nil || got.Status != payment.StatusPaid {
		t.Fatalf("order after ghost confirm: %v %s (want provider-reconciled paid)", err, got.Status)
	}
}
