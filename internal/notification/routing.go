package notification

import (
	"errors"
	"fmt"
)

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

func notificationText(eventType, orderID string) (string, error) {
	var text string
	switch eventType {
	case "order_created":
		text = "Ваш заказ создан"
	case "payment_received":
		text = "Оплата получена"
	case "order_shipped":
		text = "Ваш заказ отправлен"
	default:
		return "", fmt.Errorf("unknown event type %q", eventType)
	}
	if orderID != "" {
		text += " (заказ " + orderID + ")"
	}
	return text, nil
}
