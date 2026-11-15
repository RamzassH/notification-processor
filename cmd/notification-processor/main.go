package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RamzassH/notification-processor/internal/notification"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

const (
	serviceName = "notification-processor"
	eventsTopic = "user-actions"
)

func run(ctx context.Context, dbURL string) error {
	startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(startupCtx, dbURL)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()

	if err := checkPostgres(startupCtx, pool); err != nil {
		return err
	}

	broker := os.Getenv("KAFKA_BROKER")
	if err := checkKafka(startupCtx, broker, eventsTopic); err != nil {
		return err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          eventsTopic,
		GroupID:        serviceName,
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

func main() {
	slog.Info("start", "service", serviceName)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		slog.Error("Missing DATABASE_URL")
		os.Exit(1)
	}
	err := run(ctx, dbURL)
	stop()
	if err != nil {
		slog.Error("service failed", "error", err)
		os.Exit(1)
	}
}
