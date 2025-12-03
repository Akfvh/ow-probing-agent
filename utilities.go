package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"fmt"
	"os"
	"sync"
	"time"
	"log"
)

type CommitPayload struct {
	ContainerID string `json:"containerId"`
	NewLimitBytes int64 `json:"newLimitBytes"`
}

type ContainerReclaim struct {
	ContainerID string
	UserMax int64
	CurrentLimit int64
	ReclaimedMemory int64
	State ProbeState
	Category ProbeCategory
}

type ReclaimedSnapshot struct {
	Timestamp time.Time
	TotalReclaimedMemory int64
	Containers []ContainerReclaim
}

type memChange struct {
	id string
	limit int64
}

type ProbeState string
const (
	ProbeIdle ProbeState = "idle"
	ProbeProbing ProbeState = "probing"
	ProbeThrottled ProbeState = "throttled"
	ProbeDisabled ProbeState = "disabled"
)

type ProbeCategory string
const (
	CategoryNoDownsize ProbeCategory = "no_downsize"
	CategoryLight ProbeCategory = "light"
	CategoryMedium ProbeCategory = "medium"
	CategoryHeavy ProbeCategory = "heavy"
)

type ContainerState struct {
	ContainerID string

	// Limits
	UserMax int64
	CurrentLimit int64
	TargetLimit int64
	FinalTargetLimit int64

	// Heuristics - control
	Ssthresh int64
	LastKnownPeak int64

	// Invocation count
	InvocationCount int64
	StepInvocationCount int64

	// Probing States
	Category ProbeCategory
	State ProbeState
	
	// Timing
	ProbingStartTime time.Time
	LastThrottleTime time.Time
	ThrottleCount int
	ProbeInterval time.Duration
	LastCommitTime time.Time

	// PSI 
	psiFD int

	lastThrottledLimit int64
	consecutiveThrottles int

	committed bool
	ProbeTime int
}

var (
	containersMu sync.RWMutex
	containers = make(map[string]*ContainerState)

	commitsMu sync.Mutex
	commits []ProbeCompleteReport
)

// -- heuristics --

const (
	heavyUsageRatio = 0.85
	mediumUsageRatio = 0.50

	mediumMinFractionOfMax = 0.60
	mediumMaxFractionOfMax = 0.90
	mediumSafetyMultiplier = 1.5
	lightMinFractionOfMax = 0.25
	lightMaxFractionOfMax = 0.80
	lightSafetyMultiplier = 2.0

	maxBackoffInterval = 60 * time.Second
	backoffInterval = 10 * time.Second
	backoffFactor = 1.5 // increase target limit by 50% if throttled

	minStepBytes = 16 * 1024 * 1024 // 16MB
	initialProbeInterval = 10 * time.Second
	maxThrottleBeforeDisable = 2

	InitialMarginRatio = 1.1
	MinSafetyFloorBytes = 32 * 1024 * 1024 // 32MB
)

const (
	minStepInvocationCount = 1 // min invocation count before next probe
	maxProbeStepsPerSession = 10 // safety cap
)

const (
	commitStableDuration = 10 * time.Second
	commitMinInvocations = 3
)


// push commits to bridge every 500ms
func startPushingCommits(bridgeURL string) {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		pushCommits(bridgeURL)
	}
}

func pushCommits(bridgeURL string) {
	commitsMu.Lock()
	if len(commits) == 0 {
		commitsMu.Unlock()
		return
	}

	// move current commits to a new slice
	pending := make([]ProbeCompleteReport, len(commits))
	copy(pending, commits)
	commits = nil
	commitsMu.Unlock()

	// transform to commit payload
	var payload []CommitPayload
	for _, commit := range pending {
		mbVal := int(commit.NewLimitBytes)
		if mbVal < 1 {
			mbVal = 1
		}

		payload = append(payload, CommitPayload{
			ContainerID: commit.ContainerID,
			NewLimitBytes: int64(mbVal),
		})
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failed to marshal commits: %v", err)
		restoreCommits(pending)
		return
	}

	resp, err := http.Post(bridgeURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Failed to push commits to bridge: %v", err)
		restoreCommits(pending)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Failed to push commits to bridge: %s", resp.Status)
		restoreCommits(pending)
		return
	}

	log.Printf("[Pusher] Successfully pushed %d commits to bridge", len(payload))
}

func restoreCommits(pending []ProbeCompleteReport) {
	commitsMu.Lock()
	// somehow failed to push, so defer the commits to the next tick
	commits = append(commits, pending...)
	commitsMu.Unlock()
}

func clampBytes(raw int64, min int64, max int64) int64 {
	if raw < min {
		return min
	}
	if raw > max {
		return max
	}
	return raw
}

// decide final target based on first invocation of the session
func computeSafetyFloor(userMaxBytes, firstPeakBytes int64) int64 {
    if userMaxBytes <= 0 || firstPeakBytes <= 0 {
        return userMaxBytes
    }

    ratio := float64(firstPeakBytes) / float64(userMaxBytes)

    // Heavy usage: don't mess with it
    if ratio >= 0.80 {
        return userMaxBytes
    }

	target := int64(float64(firstPeakBytes) * InitialMarginRatio)

	//clamp min
	if (target < MinSafetyFloorBytes) {
		target = MinSafetyFloorBytes
	}

	//clamp max
	if (target > userMaxBytes) {
		target = userMaxBytes
	}

	// page align
	targetAligned := (target + 4095) & ^4095

	return targetAligned
}


func setMemHigh(containerID string, targetHigh int64) {
	path := cgroupPathFor(containerID, "memory.high")
	data := []byte(fmt.Sprintf("%d", targetHigh))
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		log.Printf("Failed to set mem high for container %s: %v", containerID, err)
	}
}

func cgroupPathFor(containerID string, filename string) string {
	return fmt.Sprintf("/sys/fs/cgroup/docker/%s/%s", containerID, filename)
}

// implement tcp-reno-like algorithm
func nextProbeTarget(currentLimit, minFloor, ssthresh int64) (int64, string) {
	// Safety clamp
	if currentLimit < minFloor {
		return minFloor, "FloorReached"
	}

	var (
		nextLimit int64
		mode string
	)

	// Descent phase: exponential || additive
	if currentLimit > ssthresh { 
		mode = "SlowStart"

		gap := currentLimit - minFloor

		// Reduce by max(half the gap, minStepBytes)
		reduction := max(minStepBytes, int64(float64(gap) * 0.5))
		nextLimit = currentLimit - reduction
	} else {
		mode = "CongestionAvoidance"
		nextLimit = currentLimit - minStepBytes
	}

	// final min clamp
	if nextLimit < minFloor {
		nextLimit = minFloor
	}
	
	// page align
	nextLimitAligned := (nextLimit + 4095) & ^4095
	return nextLimitAligned, mode
}

func getControlParams(category ProbeCategory) (alpha, beta float64) {
	switch category {
	case CategoryLight: // aggressive descent. gain > safety
		return 0.7, 0.5
	case CategoryMedium: // medium descent. gain ~ safety
		return 0.5, 0.5
	default: // no downsize category. gain << safety
		return 0.0, 0.0
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func markThrottled(containerID string) {
	containersMu.Lock()
	st, ok := containers[containerID]
	if !ok {
		containersMu.Unlock()
		return
	}

	now := time.Now()
	prevState := st.State

	st.lastThrottledLimit = st.TargetLimit
	st.ThrottleCount++

	// Throttled consecutively
	if prevState == ProbeThrottled {
		st.consecutiveThrottles++
	} else {
		st.consecutiveThrottles = 1
	}

	st.State = ProbeThrottled
	st.LastThrottleTime = now

	// Reno-style backoff
	// ssthresh update
	reclaimed := st.UserMax - st.CurrentLimit
	half := reclaimed / 2
	newLimit := st.UserMax - half

	// clamp new limit
	if newLimit < st.FinalTargetLimit {
		newLimit = st.FinalTargetLimit
	}
	if newLimit < st.FinalTargetLimit {
		newLimit = st.FinalTargetLimit
	}
	if newLimit > st.UserMax {
		newLimit = st.UserMax
	}

	st.Ssthresh = newLimit

	userMax := st.UserMax
	throttleCount := st.ThrottleCount
	consecutive := st.consecutiveThrottles
	lastTarget := st.lastThrottledLimit

	containersMu.Unlock()

	// throttle handling
	setMemHigh(containerID, userMax)

	log.Printf("Container %s throttled." +
		"Last throttle time: %s\n" +
		"Throttle count: %d\n" +
		"Consecutive: %d\n" +
		"Last target: %dMB\n", 
		containerID, 
		now, 
		throttleCount, 
		consecutive, 
		lastTarget / 1024 / 1024,
	)
}

func shouldDisableProbing(st *ContainerState) bool {
	ratio := float64(st.CurrentLimit) / float64(st.UserMax)

	// disable probing if throttled near max limit
	// trivial gain from probing
	if ratio >= 0.9 {
		return true
	}

	switch st.Category {
	case CategoryLight:
		return st.ThrottleCount >= 4 || st.consecutiveThrottles >= 2
	case CategoryMedium:
		return st.ThrottleCount >= 3 || st.consecutiveThrottles >= 2
	default:
		return true // no downsize category
	}
}

func effectiveBackoffInterval(st *ContainerState) time.Duration {
	// first throttle
	if st.ThrottleCount <= 1 {
		return backoffInterval
	}

	// exponential backoff
	factor := 1 << (st.ThrottleCount - 1)
	d := time.Duration(factor) * backoffInterval
	if d > maxBackoffInterval {
		return maxBackoffInterval
	}
	return d
}

func maybeCommit(c *ContainerState, now time.Time) {
	if c.committed {
		return
	}
	if c.CurrentLimit >= c.UserMax - minStepBytes {
		return
	}
	if now.Sub(c.LastCommitTime) < commitStableDuration {
		return
	}
	if c.InvocationCount < commitMinInvocations {
		return
	}
	if !c.LastThrottleTime.IsZero() && c.LastThrottleTime.After(c.LastCommitTime) {
		return
	}

	c.committed = true
	newLimit := c.CurrentLimit

	commitsMu.Lock()
	commits = append(commits, ProbeCompleteReport{
		ContainerID: c.ContainerID,
		Downsized: true,
		NewLimitBytes: newLimit,
	})
	commitsMu.Unlock()
}

