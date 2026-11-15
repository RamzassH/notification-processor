package notification

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func Consume(ctx context.Context, reader *kafka.Reader, pool *pgxpool.Pool) error {
	if reader == nil {
		return errors.New("nil reader")
	}
	if pool == nil {
		return errors.New("nil pool")
	}

	for ctx.Err() == nil {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		messageCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err = processMessage(messageCtx, reader, pool, msg)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func processMessage(ctx context.Context, reader *kafka.Reader, pool *pgxpool.Pool, msg kafka.Message) error {
	event, channels, err := decodeEvent(msg.Value)
	if err != nil {
		slog.Warn("invalid event", "topic", msg.Topic,
			"partition", msg.Partition, "offset", msg.Offset, "error", err)
	} else if err := saveEvent(ctx, pool, event, channels); err != nil {
		return fmt.Errorf("process %s partition %d offset %d: %w",
			msg.Topic, msg.Partition, msg.Offset, err)
	}

	if err := reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit %s partition %d offset %d: %w",
			msg.Topic, msg.Partition, msg.Offset, err)
	}
	return nil
}
