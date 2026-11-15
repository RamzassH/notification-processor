package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

func checkKafka(ctx context.Context, broker, topic string) error {
	if broker == "" {
		return errors.New("missing Kafka broker address")
	}
	if topic == "" {
		return errors.New("missing Kafka topic name")
	}
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	partitions, err := dialer.LookupPartitions(ctx, "tcp", broker, topic)
	if err != nil {
		return fmt.Errorf("lookup partitions for %q: %w", topic, err)
	}

	slog.Info("kafka topic available", "topic", topic, "count partitions", len(partitions))
	return nil

}
