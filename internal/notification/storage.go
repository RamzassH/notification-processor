package notification

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func saveEvent(ctx context.Context, pool *pgxpool.Pool, event Event, channels []string) error {
	const insert = `
		WITH accepted AS (
			INSERT INTO received_events (event_id) VALUES ($1)
			ON CONFLICT (event_id) DO NOTHING
			RETURNING event_id
		)
		INSERT INTO notifications (event_id, user_id, event_type, event_timestamp, payload, channel)
		SELECT accepted.event_id, $2, $3, $4, $5, channels.channel
		FROM accepted CROSS JOIN unnest($6::text[]) AS channels(channel)`

	_, err := pool.Exec(ctx, insert, event.EventID, event.UserID, event.EventType, event.Timestamp, []byte(event.Payload), channels)
	if err != nil {
		return fmt.Errorf("save notifications for event %s: %w", event.EventID, err)
	}
	return nil
}
