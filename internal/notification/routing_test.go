package notification

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelsFor(t *testing.T) {
	tests := []struct {
		eventType string
		want      []string
	}{
		{eventType: "order_created", want: []string{"email", "push"}},
		{eventType: "payment_received", want: []string{"email"}},
		{eventType: "order_shipped", want: []string{"push", "sms"}},
	}

	for _, tc := range tests {
		t.Run(tc.eventType, func(t *testing.T) {
			got, err := channelsFor(tc.eventType)

			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestChannelsFor_UnknownEventType(t *testing.T) {
	got, err := channelsFor("order_cancelled")

	require.Error(t, err)
	assert.Nil(t, got)
}

func TestNotificationText(t *testing.T) {
	for _, tc := range []struct {
		eventType string
		text      string
	}{
		{"order_created", "Ваш заказ создан"},
		{"payment_received", "Оплата получена"},
		{"order_shipped", "Ваш заказ отправлен"},
	} {
		t.Run(tc.eventType, func(t *testing.T) {
			got, err := notificationText(tc.eventType, "")
			require.NoError(t, err)
			assert.Equal(t, tc.text, got)

			got, err = notificationText(tc.eventType, "101")
			require.NoError(t, err)
			assert.Equal(t, tc.text+" (заказ 101)", got)
		})
	}

	_, err := notificationText("order_cancelled", "101")
	require.Error(t, err)
}
