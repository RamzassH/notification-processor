package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestConsumePoisonAndReplay(t *testing.T) {
	broker, dbURL := os.Getenv("TEST_KAFKA_BROKER"), os.Getenv("TEST_DATABASE_URL")
	if broker == "" || dbURL == "" {
		t.Skip("set TEST_KAFKA_BROKER and TEST_DATABASE_URL to use the test stack")
	}
	cfg := kafkaConfig{Broker: broker, Topic: os.Getenv("TEST_KAFKA_TOPIC"), GroupID: os.Getenv("TEST_KAFKA_GROUP_ID")}
	if cfg.Topic == "" {
		cfg.Topic = "user-actions"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "notification-processor"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	defer pool.Close()
	var id string
	require.NoError(t, pool.QueryRow(ctx, "SELECT gen_random_uuid()::text").Scan(&id))
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, err := pool.Exec(cleanup, "DELETE FROM notifications WHERE event_id = $1", id)
		require.NoError(t, err)
	}()

	const partition = 0
	conn, err := kafka.DialLeader(ctx, "tcp", broker, cfg.Topic, partition)
	require.NoError(t, err)
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	require.NoError(t, conn.SetDeadline(deadline))
	event := kafka.Message{Value: []byte(fmt.Sprintf(
		`{"event_id":%q,"user_id":12345,"event_type":"order_created","timestamp":"2026-11-15T16:48:00Z","payload":{}}`, id))}
	_, err = conn.WriteMessages(kafka.Message{Value: []byte("{")}, event, event)
	require.NoError(t, err)
	end, err := conn.ReadLastOffset()
	require.NoError(t, err)

	serviceCtx, stopService := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- run(serviceCtx, dbURL, cfg) }()
	defer func() {
		stopService()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("consumer did not stop")
		}
	}()

	client := kafka.Client{Addr: kafka.TCP(broker), Timeout: 3 * time.Second}
	request := kafka.OffsetFetchRequest{GroupID: cfg.GroupID, Topics: map[string][]int{cfg.Topic: {partition}}}
	require.Eventually(t, func() bool {
		reply, err := client.OffsetFetch(ctx, &request)
		if err != nil || reply.Error != nil {
			return false
		}
		for _, p := range reply.Topics[cfg.Topic] {
			if p.Partition == partition && p.Error == nil && p.CommittedOffset >= end {
				return true
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond)

	var channels []string
	err = pool.QueryRow(ctx, `
		SELECT array_agg(channel ORDER BY channel) FROM notifications WHERE event_id = $1`, id).Scan(&channels)
	require.NoError(t, err)
	require.Equal(t, []string{"email", "push"}, channels)
	t.Logf("group committed through offset %d on partition %d; channels: %v", end-1, partition, channels)
}
