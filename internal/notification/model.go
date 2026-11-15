package notification

import (
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

var eventIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Event struct {
	EventID   string          `json:"event_id"`
	UserID    *int64          `json:"user_id"`
	EventType string          `json:"event_type"`
	Timestamp *time.Time      `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

func decodeEvent(data []byte) (Event, []string, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return Event{}, nil, err
	}
	switch {
	case !eventIDPattern.MatchString(event.EventID):
		return Event{}, nil, errors.New("event_id must be a UUID")
	case event.UserID == nil:
		return Event{}, nil, errors.New("missing user_id")
	case event.Timestamp == nil:
		return Event{}, nil, errors.New("missing timestamp")
	case event.Payload == nil:
		return Event{}, nil, errors.New("missing payload")
	}
	channels, err := channelsFor(event.EventType)
	return event, channels, err
}

type Notification struct {
	EventID string
	UserID  int64
	Channel string
	Text    string
}
