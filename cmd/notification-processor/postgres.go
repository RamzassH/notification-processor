package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

func checkPostgres(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("pool is nil")
	}
	pingErr := pool.Ping(ctx)
	if pingErr != nil {
		return fmt.Errorf("ping postgres: %w", pingErr)
	}
	slog.Info("postgres connected")

	return nil
}
