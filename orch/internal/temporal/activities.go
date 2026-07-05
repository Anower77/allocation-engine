package temporal

import (
	"context"
	"fmt"
	"hash/fnv"

	"go.temporal.io/sdk/activity"

	"github.com/devShahriar/ride-kafka-temporal/internal/model"
)

// Activities groups the side-effecting steps of the workflow. In a real app
// these would talk to databases, pricing services, driver-location services,
// etc. Here they are deterministic stubs so the demo runs with no extra deps.
type Activities struct{}

func (Activities) ValidateRider(ctx context.Context, req model.RideRequest) error {
	activity.GetLogger(ctx).Info("ValidateRider", "rider_id", req.RiderID)
	if req.RiderID == "" {
		return fmt.Errorf("rider_id is empty")
	}
	return nil
}

func (Activities) MatchDriver(ctx context.Context, req model.RideRequest) (string, error) {
	activity.GetLogger(ctx).Info("MatchDriver", "city", req.City)
	// Pick a stable "driver" from the city name so the demo is reproducible.
	h := fnv.New32a()
	_, _ = h.Write([]byte(req.City + req.RideID))
	driverID := fmt.Sprintf("driver-%d", h.Sum32()%100)
	return driverID, nil
}

func (Activities) CalculateFare(ctx context.Context, req model.RideRequest) (float64, error) {
	activity.GetLogger(ctx).Info("CalculateFare", "pickup", req.Pickup, "dropoff", req.Dropoff)
	// Toy fare: base + length-of-route heuristic.
	base := 2.50
	distance := float64(len(req.Pickup)+len(req.Dropoff)) * 0.75
	return base + distance, nil
}

func (Activities) ConfirmRide(ctx context.Context, result model.RideResult) error {
	activity.GetLogger(ctx).Info("ConfirmRide",
		"ride_id", result.RideID, "driver", result.DriverID, "fare", result.Fare)
	return nil
}
