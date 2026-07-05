// Command producer exposes a tiny HTTP API. POST /rides publishes a ride
// request onto the Kafka topic. This is the "simple publisher" in the demo.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/devShahriar/ride-kafka-temporal/internal/kafka"
	"github.com/devShahriar/ride-kafka-temporal/internal/model"
)

func main() {
	brokers := brokersFromEnv()
	producer := kafka.NewProducer(brokers)
	defer producer.Close()

	http.HandleFunc("/rides", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}

		var req model.RideRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.RideID == "" {
			req.RideID = uuid.NewString()
		}
		if req.City == "" {
			req.City = "unknown"
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := producer.Publish(ctx, req); err != nil {
			http.Error(w, "publish failed: "+err.Error(), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "published",
			"ride_id": req.RideID,
		})
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	addr := ":8080"
	log.Printf("[producer] listening on %s, brokers=%v", addr, brokers)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func brokersFromEnv() []string {
	v := os.Getenv("KAFKA_BROKERS")
	if v == "" {
		v = "localhost:9092"
	}
	return strings.Split(v, ",")
}
