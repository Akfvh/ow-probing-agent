package main


// HealthCheck
type HealthCheckResponse struct {
	Status string `json:"status"`
}

// Used for Add/Remove/Update container
type ProbingRequest struct {
	ContainerID string `json:"container_id"`
	ProbeTime int `json:"probe_time"`

	// ActionStats metrics for probing parameter adjustment (optional)
	ColdStartSensitivity float64 `json:"coldstart_sensitivity,omitempty"`
	IAT float64 `json:"iat,omitempty"` // Inter-arrival time in seconds
	CV float64 `json:"cv,omitempty"` // Coefficient of variation
	
	// Placeholder for future use
	// HistoryPeakBytes int64 `json:"history_peak_bytes,omitempty"`
}
type ProbingResponse struct {
	Status string `json:"status"`
	ContainerID string `json:"container_id"`
}

type ProbeCompleteReport struct {
	ContainerID string `json:"container_id"`
	Downsized bool `json:"downsized"`
	NewLimitBytes int64 `json:"new_limit_bytes"`
}

type ReclaimedBytesResponse struct {
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
}

type ProbeDisabledReport struct {
	ContainerID string `json:"container_id"`
	Reason string `json:"reason"`
}
