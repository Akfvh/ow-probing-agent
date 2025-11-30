package main


// HealthCheck
type HealthCheckResponse struct {
	Status string `json:"status"`
}

// Used for Add/Remove/Update container
type ProbingRequest struct {
	ContainerID string `json:"container_id"`
}
type ProbingResponse struct {
	Status string `json:"status"`
	ContainerID string `json:"container_id"`
}

type ProbeCompleteReport struct {
	ContainerID string `json:"container_id"`
	Downsized bool `json:"downsized"`
	NewLimitMB int64 `json:"new_limit_mb"`
}

type ReclaimedBytesResponse struct {
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
}
