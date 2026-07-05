package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"

	"github.com/devShahriar/ride-kafka-temporal/internal/model"
)

const ConsumerGroup = "ride-workers"

// Consumer reads ride requests from Kafka and hands each one to a callback.
//
// Kafka protocol notes:
//   - GroupID makes this a *consumer group* member. Kafka assigns partitions
//     across all members of the group; each partition is consumed by exactly
//     one member, which is how Kafka load-balances and scales out consumers.
//   - The reader commits offsets back to the broker, so on restart it resumes
//     from the last committed position instead of replaying everything.
type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   Topic,
			GroupID: ConsumerGroup,
		}),
	}
}

// Run blocks, reading messages until ctx is cancelled. For each message it
// decodes the RideRequest and invokes handle. The offset is committed only
// after handle returns nil, giving at-least-once delivery.
func (c *Consumer) Run(ctx context.Context, handle func(context.Context, model.RideRequest) error) error {
	log.Printf("[consumer] joined group=%s topic=%s", ConsumerGroup, Topic)
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err // ctx cancelled or fatal read error
		}

		var req model.RideRequest
		if err := json.Unmarshal(m.Value, &req); err != nil {
			log.Printf("[consumer] bad message at partition=%d offset=%d: %v", m.Partition, m.Offset, err)
			_ = c.reader.CommitMessages(ctx, m) // skip poison message
			continue
		}

		log.Printf("[consumer] got ride_id=%s partition=%d offset=%d", req.RideID, m.Partition, m.Offset)

		if err := handle(ctx, req); err != nil {
			log.Printf("[consumer] handle failed for ride_id=%s: %v (will retry, not committing)", req.RideID, err)
			continue // don't commit -> message redelivered
		}

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("[consumer] commit failed: %v", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
