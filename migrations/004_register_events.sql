CREATE TABLE received_events (
    event_id uuid PRIMARY KEY
);

ALTER TABLE notifications
    ALTER COLUMN payload TYPE bytea USING convert_to(payload::text, 'UTF8');

INSERT INTO received_events (event_id)
SELECT DISTINCT event_id FROM notifications;

ALTER TABLE notifications
    ADD CONSTRAINT notifications_event_fk
    FOREIGN KEY (event_id) REFERENCES received_events (event_id) ON DELETE CASCADE;
