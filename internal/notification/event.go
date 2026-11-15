package notification

import (
	"encoding/json"
	"time"
)

type Event struct {
	EventID   string          `json:"event_id"`
	UserID    int64           `json:"user_id"`
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}
