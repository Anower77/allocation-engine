// Command driverapi exposes a tiny HTTP API that returns drivers near a point
// by running a geo_distance query against the `drivers` Elasticsearch index.
//
//	GET /drivers/nearby?lat=23.8103&lon=90.4125&radius_km=5&min_rating=4.5&size=10
//
// Config via env:
//
//	ES_URL     Elasticsearch base URL   (default http://localhost:9200)
//	ES_INDEX   index name               (default drivers)
//	ADDR       listen address           (default :8090)
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/devShahriar/ride-kafka-temporal/internal/driver"
)

func main() {
	store := driver.NewStore(envOr("ES_URL", "http://localhost:9200"), envOr("ES_INDEX", "drivers"))

	http.HandleFunc("/drivers/nearby", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "use GET", http.StatusMethodNotAllowed)
			return
		}

		q := r.URL.Query()
		lat, err1 := strconv.ParseFloat(q.Get("lat"), 64)
		lon, err2 := strconv.ParseFloat(q.Get("lon"), 64)
		if err1 != nil || err2 != nil {
			http.Error(w, "lat and lon are required floats", http.StatusBadRequest)
			return
		}

		nq := driver.NearbyQuery{
			Lat:       lat,
			Lon:       lon,
			RadiusKm:  floatOr(q.Get("radius_km"), 5),
			MinRating: floatOr(q.Get("min_rating"), 0),
			Size:      intOr(q.Get("size"), 20),
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		drivers, err := store.Nearby(ctx, nq)
		if err != nil {
			http.Error(w, "query failed: "+err.Error(), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count":   len(drivers),
			"drivers": drivers,
		})
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	addr := envOr("ADDR", ":8090")
	log.Printf("[driverapi] listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func floatOr(s string, def float64) float64 {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	return def
}

func intOr(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}
