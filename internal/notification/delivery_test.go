package notification

import (
	"context"
	"testing"
)

func TestDeliveryOutcome(t *testing.T) {
	counts := make(map[error]int)
	for roll := range 100 {
		counts[deliveryOutcome(roll)]++
	}
	for outcome, want := range map[error]int{nil: 89, ErrTemporary: 10, ErrPermanent: 1} {
		if got := counts[outcome]; got != want {
			t.Errorf("outcome %v: got %d, want %d", outcome, got, want)
		}
	}
}

func TestDeliverCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := deliver(ctx, Notification{}); err != context.Canceled {
		t.Errorf("got %v, want context.Canceled", err)
	}
}
