package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Event struct {
	EventId   string          `json:"event_id"`
	UserId    int64           `json:"user_id"`
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

func saveEvent(ctx context.Context, pool *pgxpool.Pool, event Event) error {
	if event.EventId == "" {
		return errors.New("missing event_id")
	}

	channels, err := channelsFor(event.EventType)
	if err != nil {
		return err
	}

	const insert = `
		INSERT INTO notifications (event_id, user_id, event_type, event_timestamp, payload, channel)
		SELECT $1, $2, $3, $4, $5, channel
		FROM unnest($6::text[]) AS channels(channel)
		ON CONFLICT (event_id, channel) DO NOTHING`

	_, err = pool.Exec(ctx, insert, event.EventId, event.UserId, event.EventType, event.Timestamp, event.Payload, channels)
	if err != nil {
		return fmt.Errorf("save notifications for event %s: %w", event.EventId, err)
	}
	return nil
}

func channelsFor(eventType string) ([]string, error) {
	switch eventType {
	case "order_created":
		return []string{"email", "push"}, nil
	case "payment_received":
		return []string{"email"}, nil
	case "order_shipped":
		return []string{"push", "sms"}, nil
	default:
		return nil, errors.New("unknown notification type")
	}

}
