package temporal

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/devShahriar/ride-kafka-temporal/internal/model"
)

const (
	TaskQueue = "ride-task-queue"
)

// RideWorkflow orchestrates a single ride request through a sequence of
// activities. Temporal guarantees each step is executed durably: if the
// worker crashes mid-way, the workflow resumes from the last completed step.
func RideWorkflow(ctx workflow.Context, req model.RideRequest) (model.RideResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("RideWorkflow started", "ride_id", req.RideID, "city", req.City)

	opts := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, opts)

	var a Activities

	// 1. Validate the rider.
	if err := workflow.ExecuteActivity(ctx, a.ValidateRider, req).Get(ctx, nil); err != nil {
		return model.RideResult{}, err
	}

	// 2. Match a driver.
	var driverID string
	if err := workflow.ExecuteActivity(ctx, a.MatchDriver, req).Get(ctx, &driverID); err != nil {
		return model.RideResult{}, err
	}

	// 3. Calculate the fare.
	var fare float64
	if err := workflow.ExecuteActivity(ctx, a.CalculateFare, req).Get(ctx, &fare); err != nil {
		return model.RideResult{}, err
	}

	// 4. Confirm the ride.
	result := model.RideResult{
		RideID:   req.RideID,
		DriverID: driverID,
		Fare:     fare,
		Status:   "CONFIRMED",
	}
	if err := workflow.ExecuteActivity(ctx, a.ConfirmRide, result).Get(ctx, nil); err != nil {
		return model.RideResult{}, err
	}

	logger.Info("RideWorkflow completed", "ride_id", req.RideID, "driver", driverID, "fare", fare)
	return result, nil
}
