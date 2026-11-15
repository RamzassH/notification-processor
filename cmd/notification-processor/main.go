package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func checkPostgres(dbURL string) error {
	if dbURL == "" {
		return errors.New("DATABASE_URL environment variable not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, poolErr := pgxpool.New(ctx, dbURL)
	if poolErr != nil {
		return fmt.Errorf("create postgres pool: %w", poolErr)
	}
	defer pool.Close()
	pingErr := pool.Ping(ctx)
	if pingErr != nil {
		return fmt.Errorf("ping postgres: %w", pingErr)
	}
	slog.Info("postgres connected")

	return nil
}

func main() {
	slog.Info("start", "service", "notification-processor")
	dbURL := os.Getenv("DATABASE_URL")

	err := checkPostgres(dbURL)
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
}
