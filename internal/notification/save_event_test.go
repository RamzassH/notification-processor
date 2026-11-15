package notification

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestSaveEventReplay(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to a migrated database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	userID := int64(12345)
	timestamp := time.Now().UTC()
	event := Event{
		UserID: &userID, EventType: "order_created", Timestamp: &timestamp,
		Payload: json.RawMessage(`{"order_id":101}`),
	}
	require.NoError(t, pool.QueryRow(ctx, "SELECT gen_random_uuid()::text").Scan(&event.EventID))
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, err := pool.Exec(cleanupCtx, "DELETE FROM received_events WHERE event_id = $1", event.EventID)
		if err != nil {
			t.Errorf("clean up test event: %v", err)
		}
	}()

	states := func() []string {
		t.Helper()
		var values []string
		err := pool.QueryRow(ctx, `
			SELECT array_agg(channel || '=' || status ORDER BY channel)
			FROM notifications WHERE event_id = $1`, event.EventID).Scan(&values)
		require.NoError(t, err)
		return values
	}

	require.NoError(t, saveEvent(ctx, pool, event, []string{"email", "push"}))
	require.Equal(t, []string{"email=pending", "push=pending"}, states())
	_, err = pool.Exec(ctx, "UPDATE notifications SET status = 'sent' WHERE event_id = $1 AND channel = 'email'", event.EventID)
	require.NoError(t, err)
	require.NoError(t, saveEvent(ctx, pool, event, []string{"email", "push"}))
	require.Equal(t, []string{"email=sent", "push=pending"}, states())
}
