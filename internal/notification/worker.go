package notification

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func RunWorker(ctx context.Context, pool *pgxpool.Pool) error {
	const idleDelay = 250 * time.Millisecond
	const attemptTimeout = 5 * time.Second

	for ctx.Err() == nil {
		attemptCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), attemptTimeout,
		)
		processed, err := processNext(attemptCtx, pool)
		cancel()

		if err != nil {
			return fmt.Errorf("process notification: %w", err)
		}
		if processed {
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(idleDelay):
		}
	}
	return nil
}

func processNext(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	return processNextWith(ctx, pool, deliver)
}

func processNextWith(ctx context.Context, pool *pgxpool.Pool, send func(context.Context, Notification) error) (bool, error) {
	const claim = `
		SELECT event_id, channel, user_id, event_type, payload, attempts
		FROM notifications
		WHERE status = 'pending' AND next_attempt_at <= now()
		ORDER BY next_attempt_at, event_id, channel
		LIMIT 1 FOR UPDATE SKIP LOCKED`

	const update = `
		UPDATE notifications SET status = $3, attempts = $4,
			next_attempt_at = CASE WHEN $3 = 'pending'
				THEN clock_timestamp() + $5::integer * interval '1 second'
				ELSE next_attempt_at END
		WHERE event_id = $1 AND channel = $2`

	var processed bool
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var msg Notification
		var eventType string
		var payload []byte
		var attempts int
		err := tx.QueryRow(ctx, claim).Scan(&msg.EventID, &msg.Channel, &msg.UserID, &eventType, &payload, &attempts)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("select notification: %w", err)
		}

		msg.Text, err = notificationText(eventType, orderIDFromPayload(payload))
		if err != nil {
			return err
		}

		if err := ctx.Err(); err != nil {
			return err
		}
		attempts++
		status := "sent"
		delaySeconds := 0
		if err := send(ctx, msg); err != nil {
			if !errors.Is(err, ErrTemporary) && !errors.Is(err, ErrPermanent) {
				return fmt.Errorf("deliver notification: %w", err)
			}
			if attempts >= 3 {
				status = "failed"
			} else {
				status = "pending"
				delaySeconds = 1 << (attempts - 1)
			}
		}

		_, err = tx.Exec(ctx, update, msg.EventID, msg.Channel, status, attempts, delaySeconds)
		if err != nil {
			return fmt.Errorf("update notification: %w", err)
		}
		processed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return processed, nil
}
