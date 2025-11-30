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
	NewLimitMB int64 `json:"newLimitMB"`
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
	ContainerID string // container ID
	UserMax int64 // Origianl memory limit, OOM threshold
	CurrentLimit int64 // Current memory limit (source of truth for memory usage)
	TargetLimit int64 // Target memory limit for probing
	FinalTargetLimit int64 // Final target memory limit

	InvocationCount int64 // Total invocations
	StepInvocationCount int64 // Invocation count since last probe

	Category ProbeCategory // Probing degree
	State ProbeState // Probing state

	ProbingStartTime time.Time // Time when probing started
	LastThrottleTime time.Time // Time when last throttled
	ThrottleCount int // Number of times throttled
	ProbeInterval time.Duration // Time between probes
	LastCommitTime time.Time // Time when last committed

	psiFD int // File descriptor for EPOLL

	lastThrottledLimit int64 // Last throttled limit
	consecutiveThrottles int // Number of consecutive throttles

	committed bool
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
)

const (
	minStepInvocationCount = 1 // min invocation count before next probe
	maxProbeStepsPerSession = 10 // safety cap
)

const (
	commitStableDuration = 10 * time.Second
	commitMinInvocations = 3
)


func startPushingCommits(bridgeURL string) {
	ticker := time.NewTicker(1 * time.Second)
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
		mbVal := int(commit.NewLimitMB)
		if mbVal < 1 {
			mbVal = 1
		}

		payload = append(payload, CommitPayload{
			ContainerID: commit.ContainerID,
			NewLimitMB: int64(mbVal),
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
func computeTargetHigh(userMaxBytes, firstPeakBytes int64) (int64, ProbeCategory) {
    if userMaxBytes <= 0 || firstPeakBytes <= 0 {
        return userMaxBytes, CategoryNoDownsize
    }

    ratio := float64(firstPeakBytes) / float64(userMaxBytes)

    // 1) Very heavy usage: >= 80% of userMax -> don't mess with it
    if ratio >= 0.80 {
        return userMaxBytes, CategoryNoDownsize
    }

    // 2) Medium: 50–80% of userMax
    if ratio >= 0.50 {
        // target ~ 1.25 * peak, but keep it between 65% and 90% of userMax
        base := int64(float64(firstPeakBytes) * 1.25)
        minB := int64(0.65 * float64(userMaxBytes))
        maxB := int64(0.90 * float64(userMaxBytes))
		valClamped := clampBytes(base, minB, maxB)
		valAligned := (valClamped + 4095) & ^4095
        return valAligned, CategoryMedium
    }

    // 3) Light: 20–50% of userMax (sharper)
    if ratio >= 0.20 {
        // more aggressive: ~1.6 * peak, clamped to [30%, 65%] of userMax
        base := int64(float64(firstPeakBytes) * 1.6)
        minB := int64(0.30 * float64(userMaxBytes))
        maxB := int64(0.65 * float64(userMaxBytes))
        valClamped := clampBytes(base, minB, maxB)
		valAligned := (valClamped + 4095) & ^4095
        return valAligned, CategoryLight
    }

    // 4) Ultra-light: <20% of userMax (very aggressive)
    // e.g. tiny peak relative to max, likely overprovisioned
    base := int64(float64(firstPeakBytes) * 2.0)
    minB := int64(0.20 * float64(userMaxBytes))
    maxB := int64(0.50 * float64(userMaxBytes))
    valClamped := clampBytes(base, minB, maxB)
	valAligned := (valClamped + 4095) & ^4095
    return valAligned, CategoryLight
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

func nextProbeTarget(currentLimit int64, finalTargetLimit int64, alpha float64, minStepBytes int64) int64 {
	if currentLimit <= finalTargetLimit {
		return currentLimit
	}
	cand := int64(float64(currentLimit)*(1-alpha) + float64(finalTargetLimit)*alpha)

	if cand < finalTargetLimit {
		cand = finalTargetLimit
	}

	step := currentLimit - cand
	if step < minStepBytes {
		cand = currentLimit - minStepBytes
		if cand < finalTargetLimit {
			cand = finalTargetLimit
		}
	}

	// page align
	cand = (cand + 4095) & ^4095

	if cand < finalTargetLimit {
		cand = finalTargetLimit
	}

	return cand
}

func alphaFor(category ProbeCategory) float64 {
	switch category {
	case CategoryLight:
		return 0.6
	case CategoryMedium:
		return 0.35
	default:
		return 0.0
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
	userMax := st.UserMax
	containersMu.Unlock()

	setMemHigh(containerID, userMax)

	log.Printf("Container %s throttled. Last throttle time: %s, Throttle count: %d", containerID, now, st.ThrottleCount)
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
		NewLimitMB: newLimit,
	})
	commitsMu.Unlock()
}

