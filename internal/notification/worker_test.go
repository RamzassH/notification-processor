package notification

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func workerTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL worker tests")
	}
	admin, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	schema := fmt.Sprintf("worker_test_%d", time.Now().UnixNano())
	name := pgx.Identifier{schema}.Sanitize()
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := admin.Exec(cleanup, "DROP SCHEMA "+name+" CASCADE")
		require.NoError(t, err)
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	files, err := filepath.Glob("../../migrations/*.sql")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		sql, err := os.ReadFile(file)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, string(sql))
		require.NoError(t, err, file)
	}
	return pool
}

func seedWorkerQueue(t *testing.T, ctx context.Context, pool *pgxpool.Pool, channel string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO received_events (event_id) VALUES ('00000000-0000-4000-8000-000000000101')
		ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO notifications (event_id, channel, user_id, event_type, payload, event_timestamp)
		VALUES ('00000000-0000-4000-8000-000000000101', $1, 42, 'order_created', '{"order_id":101}', now())`, channel)
	require.NoError(t, err)
}

func TestWorkerRetries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failure    error
		final      error
		wantStatus string
	}{
		{"temporary then success", ErrTemporary, nil, "sent"},
		{"temporary exhausted", ErrTemporary, ErrTemporary, "failed"},
		{"permanent exhausted", ErrPermanent, ErrPermanent, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			pool := workerTestPool(t, ctx)
			seedWorkerQueue(t, ctx, pool, "email")
			for attempt := 1; attempt <= 3; attempt++ {
				var before time.Time
				require.NoError(t, pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&before))
				processed, err := processNextWith(ctx, pool, func(_ context.Context, msg Notification) error {
					require.Equal(t, int64(42), msg.UserID)
					require.Equal(t, "email", msg.Channel)
					require.Contains(t, msg.Text, "101")
					if attempt == 3 {
						return tc.final
					}
					return tc.failure
				})
				require.NoError(t, err)
				require.True(t, processed)
				var status string
				var attempts int
				var due, after time.Time
				require.NoError(t, pool.QueryRow(ctx,
					"SELECT status, attempts, next_attempt_at, clock_timestamp() FROM notifications").Scan(&status, &attempts, &due, &after))
				require.Equal(t, attempt, attempts)
				if attempt == 3 {
					require.Equal(t, tc.wantStatus, status)
				} else {
					require.Equal(t, "pending", status)
					delay := time.Duration(attempt) * time.Second
					require.False(t, due.Before(before.Add(delay)))
					require.False(t, due.After(after.Add(delay)))
					_, err = pool.Exec(ctx, "UPDATE notifications SET next_attempt_at = now()")
					require.NoError(t, err)
				}
			}
			processed, err := processNext(ctx, pool)
			require.NoError(t, err)
			require.False(t, processed)
		})
	}
}

func TestWorkerSkipLockedAndRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	pool := workerTestPool(t, ctx)
	seedWorkerQueue(t, ctx, pool, "email")
	seedWorkerQueue(t, ctx, pool, "push")

	firstCtx, stop := context.WithCancel(ctx)
	defer stop()
	firstChannel := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		_, err := processNextWith(firstCtx, pool, func(ctx context.Context, msg Notification) error {
			firstChannel <- msg.Channel
			<-ctx.Done()
			return ctx.Err()
		})
		done <- err
	}()
	var locked string
	select {
	case locked = <-firstChannel:
	case err := <-done:
		t.Fatalf("first worker exited before delivery: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	var other string
	processed, err := processNextWith(ctx, pool, func(_ context.Context, msg Notification) error {
		other = msg.Channel
		return nil
	})
	require.NoError(t, err)
	require.True(t, processed)
	require.NotEqual(t, locked, other)
	stop()
	require.ErrorIs(t, <-done, context.Canceled)
	var status string
	var attempts int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT status, attempts FROM notifications WHERE channel=$1", locked).Scan(&status, &attempts))
	require.Equal(t, "pending", status)
	require.Zero(t, attempts)

	_, err = pool.Exec(ctx, "UPDATE notifications SET next_attempt_at=now()+interval '1 hour' WHERE channel=$1", locked)
	require.NoError(t, err)
	processed, err = processNext(ctx, pool)
	require.NoError(t, err)
	require.False(t, processed)
}
