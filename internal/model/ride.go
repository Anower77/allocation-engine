package model

// RideRequest is the message that travels over Kafka.
// The Publisher (producer) marshals it to JSON and writes it to a topic;
// the Consumer reads it back and starts a Temporal workflow with it.
type RideRequest struct {
	RideID   string `json:"ride_id"`
	RiderID  string `json:"rider_id"`
	City     string `json:"city"`     // used as the Kafka partition key
	Pickup   string `json:"pickup"`
	Dropoff  string `json:"dropoff"`
}

// RideResult is what the Temporal workflow returns once the ride is matched.
type RideResult struct {
	RideID   string  `json:"ride_id"`
	DriverID string  `json:"driver_id"`
	Fare     float64 `json:"fare"`
	Status   string  `json:"status"`
}
