ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_attempts_range,
    ADD CONSTRAINT notifications_attempts_range
        CHECK (attempts BETWEEN 0 AND 3);