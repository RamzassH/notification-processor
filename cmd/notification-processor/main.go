package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	slog.Info("start", "service", serviceName)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		slog.Error("Missing DATABASE_URL")
		os.Exit(1)
	}
	cfg := kafkaConfig{Broker: os.Getenv("KAFKA_BROKER"), Topic: os.Getenv("KAFKA_TOPIC"), GroupID: os.Getenv("KAFKA_GROUP_ID")}
	err := run(ctx, dbURL, cfg)
	stop()
	if err != nil {
		slog.Error("service failed", "error", err)
		os.Exit(1)
	}
}
