// Package driver is a thin Elasticsearch-backed store for the `drivers` index.
// It talks to ES over plain HTTP (no client library) to keep dependencies
// minimal, mirroring the rest of this repo.
package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/devShahriar/ride-kafka-temporal/internal/model"
)

// Store queries the `drivers` index on an Elasticsearch node.
type Store struct {
	baseURL string       // e.g. http://localhost:9200
	index   string       // e.g. drivers
	http    *http.Client
}

// NewStore builds a Store pointing at baseURL (no trailing slash) and index.
func NewStore(baseURL, index string) *Store {
	return &Store{
		baseURL: baseURL,
		index:   index,
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

// NearbyQuery describes a geo_distance search: drivers within radiusKm of
// (Lat, Lon), optionally filtered to a minimum rating, nearest first.
type NearbyQuery struct {
	Lat       float64
	Lon       float64
	RadiusKm  float64
	MinRating float64 // 0 = no rating filter
	Size      int     // max hits (defaults to 20 if <= 0)
}

// esResponse is the slice of the ES search response we care about.
type esResponse struct {
	Hits struct {
		Hits []struct {
			Source model.Driver `json:"_source"`
			Sort   []float64    `json:"sort"` // [distance_km] from _geo_distance sort
		} `json:"hits"`
	} `json:"hits"`
}

// Nearby runs the geo_distance search and returns drivers sorted by distance.
func (s *Store) Nearby(ctx context.Context, q NearbyQuery) ([]model.NearbyDriver, error) {
	if q.Size <= 0 {
		q.Size = 20
	}

	// Build the geo_distance filter, plus an optional rating range filter.
	filters := []map[string]any{
		{"geo_distance": map[string]any{
			"distance": fmt.Sprintf("%gkm", q.RadiusKm),
			"location": map[string]float64{"lat": q.Lat, "lon": q.Lon},
		}},
	}
	if q.MinRating > 0 {
		filters = append(filters, map[string]any{
			"range": map[string]any{
				"driver_rating": map[string]any{"gte": q.MinRating},
			},
		})
	}

	body := map[string]any{
		"size": q.Size,
		"query": map[string]any{
			"bool": map[string]any{"filter": filters},
		},
		"sort": []any{
			map[string]any{"_geo_distance": map[string]any{
				"location": map[string]float64{"lat": q.Lat, "lon": q.Lon},
				"order":    "asc",
				"unit":     "km",
			}},
		},
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	url := fmt.Sprintf("%s/%s/_search", s.baseURL, s.index)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call elasticsearch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var msg bytes.Buffer
		_, _ = msg.ReadFrom(resp.Body)
		return nil, fmt.Errorf("elasticsearch returned %d: %s", resp.StatusCode, msg.String())
	}

	var es esResponse
	if err := json.NewDecoder(resp.Body).Decode(&es); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	out := make([]model.NearbyDriver, 0, len(es.Hits.Hits))
	for _, h := range es.Hits.Hits {
		nd := model.NearbyDriver{Driver: h.Source}
		if len(h.Sort) > 0 {
			nd.DistanceKm = h.Sort[0]
		}
		out = append(out, nd)
	}
	return out, nil
}
