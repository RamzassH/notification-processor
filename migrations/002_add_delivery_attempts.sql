ALTER TABLE notifications
    ADD COLUMN IF NOT EXISTS attempts int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_notifications_next_attempt
    ON notifications (next_attempt_at)
    WHERE status = 'pending';
