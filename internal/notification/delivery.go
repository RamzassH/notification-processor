package notification

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
)

var (
	ErrTemporary = errors.New("temporary delivery failure")
	ErrPermanent = errors.New("permanent delivery failure")
)

type Notification struct {
	EventID string
	UserID  int64
	Channel string
	Text    string
}

func deliver(ctx context.Context, msg Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := deliveryOutcome(rand.IntN(100)); err != nil {
		return err
	}
	slog.InfoContext(ctx, "notification sent", "event_id", msg.EventID,
		"user_id", msg.UserID, "channel", msg.Channel, "text", msg.Text)
	return nil
}

func deliveryOutcome(roll int) error {
	switch {
	case roll == 0:
		return ErrPermanent
	case roll <= 10:
		return ErrTemporary
	default:
		return nil
	}
}
