// Command worker hosts the Temporal workflow and activity code. It polls the
// task queue and executes RideWorkflow runs that the consumer enqueues.
package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/worker"

	"github.com/devShahriar/ride-kafka-temporal/internal/temporal"
)

func main() {
	temporalHost := getenv("TEMPORAL_HOSTPORT", "localhost:7233")

	c, err := temporal.NewClient(temporalHost)
	if err != nil {
		log.Fatalf("[worker] temporal dial failed: %v", err)
	}
	defer c.Close()

	w := worker.New(c, temporal.TaskQueue, worker.Options{})
	w.RegisterWorkflow(temporal.RideWorkflow)
	w.RegisterActivity(&temporal.Activities{})

	log.Printf("[worker] polling task queue %q on %s", temporal.TaskQueue, temporalHost)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("[worker] run error: %v", err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
