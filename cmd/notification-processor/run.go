package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/RamzassH/notification-processor/internal/notification"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

const serviceName = "notification-processor"

type kafkaConfig struct {
	Broker  string
	Topic   string
	GroupID string
}

func run(ctx context.Context, dbURL string, cfg kafkaConfig) error {
	if cfg.GroupID == "" {
		return errors.New("missing KAFKA_GROUP_ID")
	}
	startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(startupCtx, dbURL)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(startupCtx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	slog.Info("postgres connected")

	if err := checkKafka(startupCtx, cfg.Broker, cfg.Topic); err != nil {
		return err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.Broker},
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		CommitInterval: 0,
	})
	defer reader.Close()

	slog.Info("ready", "service", serviceName)

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return notification.Consume(groupCtx, reader, pool)
	})

	const workerCount = 2
	for range workerCount {
		group.Go(func() error {
			return notification.RunWorker(groupCtx, pool)
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	slog.Info("service stopped", "service", serviceName)
	return nil
}

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

	slog.Info("kafka topic available", "topic", topic, "partitions", len(partitions))
	return nil
}
