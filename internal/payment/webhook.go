package payment

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/mash-cloud/mash-api/internal/observability"
	"go.opentelemetry.io/otel/attribute"

	"github.com/mash-cloud/mash-api/internal/ids"
)

// HandleWebhook processes a provider webhook end-to-end:
//
//	per-IP budget → HMAC over RAW bytes → parse → resolve order →
//	dedupe by event id → amount cross-check → state-machine apply → 200
//
// Delivery contract is at-least-once: duplicates are acknowledged and
// ignored. Ordering is NOT guaranteed by providers: a payment.failed may
// land after payment.captured — the state machine drops it (paid is
// absorbing), which is exactly why ordering must never matter here.
//
// The caller (api layer) answers 200 on nil error only — i.e. after the
// state change has COMMITTED. Anything else (5xx) makes the provider
// retry, which is the desired behavior for transient storage failures.
func (s *Service) HandleWebhook(ctx context.Context, raw []byte, signature, clientIP string) error {
	if !s.Enabled() {
		return ErrDisabled
	}

	// Budget first (cheap): a webhook flood must not reach HMAC+DB.
	if s.lim != nil && s.cfg.WebhookPerIP > 0 {
		if !s.lim.Admit(ctx, webhookBudgetKey(clientIP), time.Minute, s.cfg.WebhookPerIP,
			ids.New("wh")) {
			return errWebhookBudget
		}
	}

	// Signature over the exact raw bytes. Never a re-marshal: Go map
	// iteration order would change the payload and break the MAC. This is
	// the route's ONLY authentication — fail closed, before any parse or
	// store access.
	if !s.provider.VerifyWebhookSignature(raw, signature) {
		s.webhookMetric("rejected")
		return ErrWebhookSignature()
	}

	ev, err := s.provider.ParseWebhook(raw)
	if err != nil || ev == nil || ev.EventID == "" {
		// Correctly signed garbage: a provider bug or a versioned envelope
		// we do not understand. Acknowledge (stop retries) and count it —
		// silent drops would hide provider-side drift.
		s.webhookMetric("unparsed")
		return nil
	}

	// Resolve the local order BEFORE recording (read-only; the resolved
	// id feeds the event's FK and the amount cross-check below).
	var o *Order
	if ev.ProviderOrderID != "" {
		resolved, rerr := s.store.OrderByProviderID(ctx, ev.Provider, ev.ProviderOrderID)
		if rerr == nil {
			o, ev.LocalOrderID = resolved, resolved.ID
		} else if !errors.Is(rerr, ErrNotFound) {
			return rerr // storage failure: 5xx → provider retries → correct
		}
	}

	// Dedupe: the event id is globally unique at the provider. The
	// INSERT ... ON CONFLICT DO NOTHING verdict IS the dedupe — atomic
	// even under concurrent duplicate deliveries on different instances.
	duplicate, err := s.store.RecordWebhookEvent(ctx, ev)
	if err != nil {
		return err // 5xx → provider retries → correct (not yet recorded)
	}
	if duplicate {
		s.webhookMetric("duplicate", observability.Attr("event", ev.Type))
		// Audit H4 — converge, do not merely acknowledge. The first delivery
		// may have recorded this event and died BEFORE applying it (rolling
		// deploy, OOM — a millisecond window). The provider's retry used to be
		// swallowed right here by our own dedupe, permanently losing a captured
		// payment the system was told about TWICE. For paid-class events whose
		// order is still not paid, apply now: applyPaid is a state-machine
		// conditional UPDATE (paid is absorbing), so this can never double-apply
		// — it only closes the recorded-but-unapplied gap.
		if o != nil && (ev.Type == EventPaymentCaptured || ev.Type == EventOrderPaid) && o.Status != StatusPaid {
			if ev.AmountPaise > 0 && ev.AmountPaise != o.AmountPaise {
				s.webhookMetric("amount_mismatch", observability.Attr("event", ev.Type))
				return ErrAmountMismatch(o.AmountPaise, ev.AmountPaise)
			}
			if _, err := s.applyPaid(ctx, o.ID, ev.ProviderPaymentID); err != nil {
				return err // 5xx → provider retries again → correct
			}
			s.webhookMetric("converged_duplicate", observability.Attr("event", ev.Type))
		}
		return nil
	}

	if !ev.Handled() {
		// Recorded for audit, acted on never (refund reversal is v2 — the
		// ledger kind enum already reserves it).
		s.webhookMetric("ignored", observability.Attr("event", ev.Type))
		// Audit L16: every refund is currently a free plan extension until
		// someone manually reconciles. Make that VISIBLE: a dedicated outcome +
		// a loud warn (rate-friendly: one line per event) so the daily
		// refunds-vs-active-entitlements review has a hook to start from.
		if strings.Contains(ev.Type, "refund") || strings.Contains(ev.Type, "charge.disputed") {
			s.webhookMetric("refund_recorded", observability.Attr("event", ev.Type))
			observability.LogWarn("payment: refund/dispute recorded — entitlement NOT reversed (v2); manual reconciliation required",
				"event_id", ev.EventID, "event_type", ev.Type, "order_id", ev.LocalOrderID)
		}
		return nil
	}

	if o == nil {
		// Unknown order: not ours (another product on the same webhook
		// URL?) or pre-dating a data migration. The event is recorded;
		// acknowledging stops retry storms. Ops can replay from the
		// recorded payload.
		s.webhookMetric("orphan", observability.Attr("event", ev.Type))
		return nil
	}

	// Amount cross-check (tamper alarm): a correctly-signed event carrying
	// the wrong amount for the order it references is a red flag — the
	// event is recorded, the order stays untouched, the 409-class error
	// surfaces for ops. (Applies to handled events only; recorded-only
	// events keep their own amounts.)
	if ev.AmountPaise > 0 && ev.AmountPaise != o.AmountPaise {
		s.webhookMetric("amount_mismatch", observability.Attr("event", ev.Type))
		return ErrAmountMismatch(o.AmountPaise, ev.AmountPaise)
	}

	switch ev.Type {
	case EventPaymentCaptured, EventOrderPaid:
		if _, err := s.applyPaid(ctx, o.ID, ev.ProviderPaymentID); err != nil {
			return err
		}
		s.webhookMetric("applied", observability.Attr("event", ev.Type))
	case EventPaymentFailed:
		if _, err := s.applyFailed(ctx, o.ID, "payment.failed"); err != nil {
			return err
		}
		s.webhookMetric("applied", observability.Attr("event", ev.Type))
	}
	return nil
}

func (s *Service) webhookMetric(outcome string, attrs ...attribute.KeyValue) {
	if s.metrics == nil || s.metrics.PaymentWebhooks == nil {
		return
	}
	all := append([]attribute.KeyValue{observability.Attr("outcome", outcome)}, attrs...)
	s.metrics.PaymentWebhooks.Add(context.Background(), 1, all...)
}
