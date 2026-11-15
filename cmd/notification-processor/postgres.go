package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

func checkPostgres(ctx context.Context, dbURL string) error {
	if dbURL == "" {
		return errors.New("DATABASE_URL environment variable not set")
	}

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
