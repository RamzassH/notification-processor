package notification

import "errors"

func channelsFor(eventType string) ([]string, error) {
	switch eventType {
	case "order_created":
		return []string{"email", "push"}, nil
	case "payment_received":
		return []string{"email"}, nil
	case "order_shipped":
		return []string{"push", "sms"}, nil
	default:
		return nil, errors.New("unknown notification type")
	}
}
