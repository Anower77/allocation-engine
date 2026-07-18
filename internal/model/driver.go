package model

// GeoPoint is Elasticsearch's native geo_point object shape: { "lat", "lon" }.
type GeoPoint struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// Driver mirrors a document in the `drivers` index. `Location` is mapped as a
// geo_point in Elasticsearch, so geo_distance / geo_bounding_box queries run
// directly against it.
type Driver struct {
	Name   string   `json:"driver_name"`
	Rating float64  `json:"driver_rating"`
	Review string   `json:"driver_review"`
	Phone  string   `json:"phone"`
	Location GeoPoint `json:"location"`
}

// NearbyDriver is a Driver plus how far it sits from the query point (km).
// The distance comes from Elasticsearch's _geo_distance sort value.
type NearbyDriver struct {
	Driver
	DistanceKm float64 `json:"distance_km"`
}
