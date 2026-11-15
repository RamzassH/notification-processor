package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run(ctx context.Context) error {
	startupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := checkPostgres(startupCtx, os.Getenv("DATABASE_URL")); err != nil {
		return err
	}
	if err := checkKafka(startupCtx, os.Getenv("KAFKA_BROKER"), "user-actions"); err != nil {
		return err
	}

	slog.Info("ready", "service", "notification-processor")
	<-ctx.Done()
	slog.Info("service stopped", "service", "notification-processor")
	return nil
}

func main() {
	slog.Info("start", "service", "notification-processor")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
}
