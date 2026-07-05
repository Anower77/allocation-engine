// Command consumer reads ride requests from Kafka and starts a Temporal
// workflow for each one. This is the bridge between the Kafka protocol world
// (topics/partitions/offsets) and the Temporal durable-execution world.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go.temporal.io/sdk/client"

	"github.com/devShahriar/ride-kafka-temporal/internal/kafka"
	"github.com/devShahriar/ride-kafka-temporal/internal/model"
	"github.com/devShahriar/ride-kafka-temporal/internal/temporal"
)

func main() {
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	temporalHost := getenv("TEMPORAL_HOSTPORT", "localhost:7233")

	tc, err := temporal.NewClient(temporalHost)
	if err != nil {
		log.Fatalf("[consumer] temporal dial failed: %v", err)
	}
	defer tc.Close()

	consumer := kafka.NewConsumer(brokers)
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// For each Kafka message, kick off a Temporal workflow. The Kafka offset is
	// committed only after ExecuteWorkflow succeeds (handle returns nil), so a
	// crash before the workflow is started simply redelivers the message.
	handle := func(ctx context.Context, req model.RideRequest) error {
		we, err := tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:        "ride-" + req.RideID, // dedupe: same ride => same workflow id
			TaskQueue: temporal.TaskQueue,
		}, temporal.RideWorkflow, req)
		if err != nil {
			return err
		}
		log.Printf("[consumer] started workflow id=%s run=%s for ride_id=%s",
			we.GetID(), we.GetRunID(), req.RideID)
		return nil
	}

	log.Printf("[consumer] starting, brokers=%v temporal=%s", brokers, temporalHost)
	if err := consumer.Run(ctx, handle); err != nil && ctx.Err() == nil {
		log.Fatalf("[consumer] run error: %v", err)
	}
	log.Println("[consumer] shut down")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
