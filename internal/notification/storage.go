package notification

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func saveEvent(ctx context.Context, pool *pgxpool.Pool, event Event) error {
	if event.EventID == "" {
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

	_, err = pool.Exec(ctx, insert, event.EventID, event.UserID, event.EventType, event.Timestamp, event.Payload, channels)
	if err != nil {
		return fmt.Errorf("save notifications for event %s: %w", event.EventID, err)
	}
	return nil
}
