package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/devShahriar/ride-kafka-temporal/internal/model"
)

const Topic = "ride-requests"

// Producer is a thin wrapper around a kafka-go Writer.
//
// Kafka protocol notes:
//   - A Writer speaks the Kafka wire protocol over TCP to the broker(s).
//   - We set the message Key to the city. Kafka hashes the key to choose a
//     partition, so every request for the same city lands on the same
//     partition and is therefore consumed in order.
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  Topic,
			Balancer:               &kafka.Hash{}, // partition = hash(key) % numPartitions
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafka.RequireAll, // wait for all in-sync replicas
		},
	}
}

func (p *Producer) Publish(ctx context.Context, req model.RideRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(req.City), // partition key
		Value: body,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return err
	}

	log.Printf("[producer] published ride_id=%s city=%s (key=%s)", req.RideID, req.City, req.City)
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
