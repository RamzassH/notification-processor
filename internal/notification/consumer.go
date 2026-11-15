package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

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

	for {
		msg, err := reader.FetchMessage(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("fetch message: %w", err)
		}

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Warn("invalid event JSON", "topic", msg.Topic,
				"partition", msg.Partition, "offset", msg.Offset, "error", err)
		} else if err := saveEvent(ctx, pool, event); err != nil {
			return fmt.Errorf("process %s partition %d offset %d: %w", msg.Topic, msg.Partition, msg.Offset, err)
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit %s partition %d offset %d: %w", msg.Topic, msg.Partition, msg.Offset, err)
		}
	}
}
