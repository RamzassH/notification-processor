ALTER TABLE notifications
    ADD CONSTRAINT notifications_attempts_range
        CHECK (attempts BETWEEN 0 AND 3);