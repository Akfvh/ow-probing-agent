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
	Sensitivity float64
	IAT float64 // Inter-arrival time in seconds (from ActionStats)
	CV float64  // Coefficient of variation (from ActionStats)

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
	backoffCount int
	ProbeInterval time.Duration
	LastCommitTime time.Time

	// PSI 
	psiFD int

	lastThrottledLimit int64
	consecutiveThrottles int

	// committed bool
	LastCommittedLimit int64
	ProbeTime int
}

var (
	containersMu sync.RWMutex
	containers = make(map[string]*ContainerState)

	commitsMu sync.Mutex
	commits []ProbeCompleteReport
	
	bridgeBaseURLForDisabled string
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

	maxBackoffCount = 8
	backoffCount = 1

	minStepBytes = 16 * 1024 * 1024 // 16MB
	initialProbeInterval = 3 * time.Second
	maxThrottleBeforeDisable = 2

	InitialMarginRatio = 1.20 // 20% headroom
	MinSafetyFloorBytes = 32 * 1024 * 1024 // 32MB
	
	// ProbeInterval bounds based on IAT
	minProbeInterval = 1 * time.Second   // Minimum: 1 second
	maxProbeInterval = 10 * time.Second  // Maximum: 10 seconds
	probeIntervalRatio = 0.3             // ProbeInterval = IAT * ratio (clamped)
	
	// ssthresh calculation: always based on FinalTargetLimit
	// ssthresh is the "fine-tuning start point" - should be close to finalTarget
	// to avoid making OomAvoidance phase too long
	ssthreshMarginRatio = 1.10 // ssthresh = FinalTargetLimit * 1.10 (10% margin above final target)
)

const (
	minStepInvocationCount = 3 // min invocation count before next probe (for stability)
	maxProbeStepsPerSession = 10 // safety cap
)

const (
	commitStableDuration = 3 * time.Second
	commitMinInvocations = 3
	intermediateCommitNoTrafficDuration = 15 * time.Second // commit if no traffic for this duration
	// Intermediate commit memory gain thresholds
	intermediateCommitMinSavedBytes = 128 * 1024 * 1024 // 128MB minimum saved bytes
	intermediateCommitMinSavedRatio = 0.20 // 20% minimum saved ratio (savedBytes / LastCommittedLimit)
)


// push commits to bridge every 500ms
func startPushingCommits(bridgeURL string) {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		pushCommits(bridgeURL)
	}
}

// Batch and push commits message
func pushCommits(bridgeURL string) {
	commitsMu.Lock()
	if len(commits) == 0 {
		commitsMu.Unlock()
		return
	}

	// move current commits to a new slice to avoid race condition
	pending := make([]ProbeCompleteReport, len(commits))
	copy(pending, commits)
	commits = nil
	commitsMu.Unlock()

	// transform to commit payload
	var payload []CommitPayload
	for _, commit := range pending {
		newLimitBytes := int(commit.NewLimitBytes)
		if newLimitBytes < 1 {
			newLimitBytes = 1
		}

		payload = append(payload, CommitPayload{
			ContainerID: commit.ContainerID, 
			NewLimitBytes: int64(newLimitBytes),
		})
	}

	// marshal to json
	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failed to marshal commits: %v", err)
		restoreCommits(pending)
		return
	}

	// post to bridge (batch request)
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

	// commit memory.max and update UserMax in container state
	for _, p := range pending {
		setMemMax(p.ContainerID, p.NewLimitBytes)
		
		// Update UserMax in container state to maintain consistency
		containersMu.Lock()
		if container, ok := containers[p.ContainerID]; ok {
			container.UserMax = p.NewLimitBytes
			container.LastCommittedLimit = p.NewLimitBytes
		}
		containersMu.Unlock()
	}
	// log.Printf("[Pusher] Successfully pushed %d commits to bridge", len(payload))
}

// Restore commits that we failed to push to bridge
func restoreCommits(pending []ProbeCompleteReport) {
	commitsMu.Lock()
	// somehow failed to push, so defer the commits to the next tick
	commits = append(commits, pending...)
	commitsMu.Unlock()
}

// Notify bridge that probing is disabled for a container
func notifyProbeDisabled(containerID string, reason string) {
	if bridgeBaseURLForDisabled == "" {
		log.Printf("Bridge base URL not set, skipping probe disabled notification for container %s", containerID)
		return
	}
	
	report := ProbeDisabledReport{
		ContainerID: containerID,
		Reason: reason,
	}
	
	jsonData, err := json.Marshal(report)
	if err != nil {
		log.Printf("Failed to marshal probe disabled report: %v", err)
		return
	}
	
	url := bridgeBaseURLForDisabled + "/probeDisabled"
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Failed to notify bridge about probe disabled for container %s: %v", containerID, err)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		log.Printf("Failed to notify bridge about probe disabled: %s", resp.Status)
		return
	}
	
	log.Printf("Notified bridge that probing is disabled for container %s (reason: %s)", containerID, reason)
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

// decide final target based on first invocation peak / history peak ratio
func computeFinalTarget(userMaxBytes, peakBytes int64) int64 {
    if userMaxBytes <= 0 || peakBytes <= 0 {
        return userMaxBytes
    }

    ratio := float64(peakBytes) / float64(userMaxBytes)

    // Heavy usage: don't mess with it
    if ratio >= 0.80 {
        return userMaxBytes
    }

	target := int64(float64(peakBytes) * InitialMarginRatio)

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
		// log.Printf("Failed to set mem high for container %s: %v", containerID, err)
	}
}

// setMemHighToMax sets memory.high to "max" (unlimited, bounded by memory.max)
// This allows OOM to occur if memory usage exceeds memory.max, preventing infinite lagging
func setMemHighToMax(containerID string) {
	path := cgroupPathFor(containerID, "memory.high")
	data := []byte("max")
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		log.Printf("Failed to set mem high to max for container %s: %v", containerID, err)
	}
}

func cgroupPathFor(containerID string, filename string) string {
	return fmt.Sprintf("/sys/fs/cgroup/docker/%s/%s", containerID, filename)
}

// implement tcp-reno-like algorithm
func nextProbeTarget(currentLimit, finalTarget, ssthresh int64, sensitivity float64) (int64, string) {
	// Safety clamp
	if currentLimit < finalTarget {
		return finalTarget, "FinalTargetReached"
	}

	var (
		nextLimit int64
		mode string
	)

	// step size = 5% of current limit (for OomAvoidance)
	baseStep := float64(currentLimit) * 0.05

	// clamp
	if baseStep < 16*1024*1024 { baseStep = 16*1024*1024 }
	if baseStep > 64*1024*1024 { baseStep = 64*1024*1024 }

	// Descent phase: exponential || additive
	if currentLimit > ssthresh { 
		mode = "QuickStart"

		// Use sensitivity to adjust drop amount: higher sensitivity = more conservative
		// Default 20% drop, but adjust based on sensitivity (1.0 = normal, >1.0 = more conservative)
		baseDropRatio := 0.20
		if sensitivity > 1.0 {
			// More conservative for high sensitivity
			baseDropRatio = 0.20 / sensitivity
		} else if sensitivity < 1.0 && sensitivity > 0 {
			// More aggressive for low sensitivity
			baseDropRatio = 0.20 * (2.0 - sensitivity)
		}
		// Clamp between 10% and 30%
		if baseDropRatio < 0.10 {
			baseDropRatio = 0.10
		} else if baseDropRatio > 0.30 {
			baseDropRatio = 0.30
		}
		
		dropAmount := int64(float64(currentLimit) * baseDropRatio)
		nextLimit = currentLimit - dropAmount
	} else {
		mode = "OomAvoidance"
		// Accelerate OomAvoidance: use larger step size (10% instead of 5%)
		// This speeds up convergence to final target
		acceleratedStep := baseStep * 2.0 // Double the step size
		// Increase max clamp for OomAvoidance to allow larger steps
		maxOomAvoidanceStep := float64(128 * 1024 * 1024) // 128MB max (increased from 64MB)
		if acceleratedStep > maxOomAvoidanceStep {
			acceleratedStep = maxOomAvoidanceStep
		}
		nextLimit = currentLimit - int64(acceleratedStep)
	}

	// final min clamp
	if nextLimit < finalTarget {
		nextLimit = finalTarget
	}
	
	// page align
	nextLimitAligned := (nextLimit + 4095) & ^4095
	return nextLimitAligned, mode
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
	// prevState := st.State

	st.lastThrottledLimit = st.TargetLimit
	st.ThrottleCount++
	st.backoffCount = effectiveBackoffCount(st)

	// Track consecutive throttles
	// If throttled recently (within cooldown period), increment consecutive count
	cooldownPeriod := 30 * time.Second
	if !st.LastThrottleTime.IsZero() && now.Sub(st.LastThrottleTime) < cooldownPeriod {
		st.consecutiveThrottles++
		// log.Printf("[THROTTLE TRACKING] Container %s: consecutive throttles incremented to %d (time since last: %v)", 
		// 	containerID, st.consecutiveThrottles, now.Sub(st.LastThrottleTime))
	} else {
		// Reset if enough time has passed since last throttle
		// if !st.LastThrottleTime.IsZero() {
		// 	log.Printf("[THROTTLE TRACKING] Container %s: consecutive throttles reset to 1 (time since last: %v > %v)", 
		// 		containerID, now.Sub(st.LastThrottleTime), cooldownPeriod)
		// }
		st.consecutiveThrottles = 1
	}

	st.State = ProbeThrottled
	st.LastThrottleTime = now

	// ssthresh update: always based on FinalTargetLimit for consistency
	// ssthresh = FinalTargetLimit * marginRatio
	// This ensures ssthresh and FinalTargetLimit move together
	newSsthresh := int64(float64(st.FinalTargetLimit) * ssthreshMarginRatio)
	
	// Clamp: don't exceed UserMax
	if newSsthresh > st.UserMax {
		newSsthresh = st.UserMax
	}
	
	// Ensure ssthresh is at least FinalTargetLimit
	if newSsthresh < st.FinalTargetLimit {
		newSsthresh = st.FinalTargetLimit
	}
	
	// Page align
	newSsthresh = (newSsthresh + 4095) & ^4095
	st.Ssthresh = newSsthresh

	userMax := st.UserMax
	st.CurrentLimit = userMax
	st.TargetLimit = userMax

	containersMu.Unlock()

	// throttle handling: set memory.high to max to allow OOM instead of infinite lagging
	// When memory.high is "max", it's effectively unlimited (bounded by memory.max)
	// This ensures that if memory usage exceeds memory.max, OOM occurs instead of throttling
	setMemHighToMax(containerID)
	log.Printf("[THROTTLED] Container %s throttled at %dMB (throttle #%d, consecutive: %d, ssthresh: %dMB)", 
		containerID, st.lastThrottledLimit>>20, st.ThrottleCount, st.consecutiveThrottles, st.Ssthresh>>20)
}

func shouldDisableProbing(st *ContainerState) bool {
	// Don't disable immediately after throttle - use the limit before throttle
	// CurrentLimit is set to UserMax after throttle, so we use lastThrottledLimit instead
	var limitToCheck int64
	if st.lastThrottledLimit > 0 {
		limitToCheck = st.lastThrottledLimit
	} else {
		limitToCheck = st.CurrentLimit
	}
	
	ratio := float64(limitToCheck) / float64(st.UserMax)

	// disable probing if throttled near max limit (based on limit before throttle)
	// trivial gain from probing
	if ratio >= 0.9 {
		return true
	}

	// Hybrid disable strategy based on consecutive throttles:
	// - First throttle (consecutiveThrottles == 1): Assume spike, be lenient
	// - Second consecutive throttle (== 2): Assume workload shift, try workload_shift strategy once
	// - Third+ consecutive throttle (>= 3): Disable more aggressively
	if st.consecutiveThrottles == 1 {
		// First throttle: spike assumption - be more lenient
		// Only disable if many throttles overall
		switch st.Category {
		case CategoryLight:
			return st.ThrottleCount >= 5 // More lenient: 5 instead of 4
		case CategoryMedium:
			return st.ThrottleCount >= 4 // More lenient: 4 instead of 3
		default:
			return true // no downsize category
		}
	} else if st.consecutiveThrottles == 2 {
		// Second consecutive throttle: workload shift assumption
		// Allow one more attempt with workload_shift strategy
		// Only disable if total throttle count is very high (more lenient than consecutiveThrottles == 1)
		switch st.Category {
		case CategoryLight:
			return st.ThrottleCount >= 6 // More lenient to allow workload_shift attempt
		case CategoryMedium:
			return st.ThrottleCount >= 5 // More lenient to allow workload_shift attempt
		default:
			return true // no downsize category
		}
	} else {
		// Third+ consecutive throttle: disable aggressively
		// Workload shift strategy already tried, disable now
		switch st.Category {
		case CategoryLight:
			return st.ThrottleCount >= 3 || st.consecutiveThrottles >= 3
		case CategoryMedium:
			return st.ThrottleCount >= 2 || st.consecutiveThrottles >= 3
		default:
			return true // no downsize category
		}
	}
}

func getDisableReason(st *ContainerState) string {
	// Use lastThrottledLimit (same as shouldDisableProbing) to get accurate ratio
	var limitToCheck int64
	if st.lastThrottledLimit > 0 {
		limitToCheck = st.lastThrottledLimit
	} else {
		limitToCheck = st.CurrentLimit
	}
	
	ratio := float64(limitToCheck) / float64(st.UserMax)
	
	if ratio >= 0.9 {
		return "near_max_limit"
	}
	
	// Check consecutive throttles first (more specific reason)
	if st.consecutiveThrottles >= 3 {
		return fmt.Sprintf("consecutive_throttles_%d", st.consecutiveThrottles)
	}
	
	switch st.Category {
	case CategoryLight:
		if st.consecutiveThrottles == 1 && st.ThrottleCount >= 5 {
			return "throttled_too_many_light"
		} else if st.consecutiveThrottles == 2 && st.ThrottleCount >= 6 {
			return "throttled_too_many_light_after_workload_shift"
		} else if st.consecutiveThrottles >= 3 {
			if st.ThrottleCount >= 3 {
				return "throttled_too_many_light_consecutive"
			}
		}
	case CategoryMedium:
		if st.consecutiveThrottles == 1 && st.ThrottleCount >= 4 {
			return "throttled_too_many_medium"
		} else if st.consecutiveThrottles == 2 && st.ThrottleCount >= 5 {
			return "throttled_too_many_medium_after_workload_shift"
		} else if st.consecutiveThrottles >= 3 {
			if st.ThrottleCount >= 2 {
				return "throttled_too_many_medium_consecutive"
			}
		}
	}
	
	return "unknown"
}

func effectiveBackoffCount(st *ContainerState) int {
	// first throttle: immediate resume (spike assumption)
	if st.ThrottleCount <= 1 {
		return 1
	}

	// Hybrid backoff strategy based on consecutive throttles:
	// - First throttle (consecutiveThrottles == 1): Fast resume for spike
	// - Second consecutive throttle (== 2): Shorter backoff to try workload_shift strategy
	// - Third+ consecutive throttle (>= 3): Longer backoff for stability
	
	if st.consecutiveThrottles == 1 {
		// First throttle or reset: spike assumption, fast resume
		// Use moderate backoff based on total throttle count
		if st.ThrottleCount == 2 {
			return 2 // Short backoff
		} else if st.ThrottleCount == 3 {
			return 4 // Moderate backoff
		} else {
			// More throttles: exponential backoff
			factor := 1 << (st.ThrottleCount - 1)
			if factor > maxBackoffCount {
				return maxBackoffCount
			}
			return factor
		}
	} else if st.consecutiveThrottles == 2 {
		// Second consecutive throttle: workload shift assumption
		// Shorter backoff to try workload_shift strategy sooner
		return 3 // Moderate backoff, allow workload_shift attempt
	} else {
		// Third+ consecutive throttle: workload_shift already tried, disable soon
		// Use shorter backoff to reach disable decision faster
		// Since disable condition is already aggressive, we don't need long backoff
		if st.ThrottleCount <= 3 {
			return 4 // Moderate backoff
		} else {
			// Many throttles: still use exponential but cap it
			factor := 1 << (st.ThrottleCount - 1)
			if factor > maxBackoffCount {
				return maxBackoffCount
			}
			return factor
		}
	}
}

func maybeCommit(c *ContainerState, now time.Time) {
	// only at idle state
	if c.State != ProbeIdle {
		return
	}
	
	// Fast path: if committed recently, no need to commit again (prevent continuous commits)
	// Check this early as it's a common case and avoids unnecessary calculations
	if !c.LastCommitTime.IsZero() && now.Sub(c.LastCommitTime) < commitStableDuration {
		return
	}
	
	// Fast path: if not enough invocations, no need to commit
	if c.InvocationCount < commitMinInvocations {
		return
	}
	
	// Fast path: if throttled recently, no need to commit
	if !c.LastThrottleTime.IsZero() && c.LastThrottleTime.After(c.LastCommitTime) {
		return
	}
	
	// Check if already at max limit (simple arithmetic)
	if c.CurrentLimit >= c.UserMax - minStepBytes {
		return
	}

	// Expensive calculation: check if enough saved bytes to justify commit
	savedBytes := c.LastCommittedLimit - c.CurrentLimit
	
	// Safety check: if no memory saved or LastCommittedLimit is invalid, don't commit
	if savedBytes <= 0 || c.LastCommittedLimit <= 0 {
		return
	}
	
	ratio := float64(savedBytes) / float64(c.LastCommittedLimit)
	// Use intermediate commit thresholds for consistency
	// Commit if savedBytes >= threshold OR ratio >= threshold (OR condition)
	if savedBytes < intermediateCommitMinSavedBytes && ratio < intermediateCommitMinSavedRatio {
		return
	}

	// new limit with headroom: use CurrentLimit (proven stable) as base
	bufferedLimit := int64(float64(c.CurrentLimit) * 1.15) // 15% headroom

	// clamp: don't exceed original UserMax
	if bufferedLimit > c.UserMax {
		bufferedLimit = c.UserMax
	}

	// Update commit time (UserMax will be updated after successful bridge push)
	c.LastCommitTime = now

	commitsMu.Lock()
	commits = append(commits, ProbeCompleteReport{
		ContainerID: c.ContainerID,
		Downsized: true,
		NewLimitBytes: bufferedLimit,
	})
	commitsMu.Unlock()

	log.Printf("[COMMIT] Container %s: limit %dMB (saved %dMB, %.1f%%)", c.ContainerID, bufferedLimit / 1024 / 1024, savedBytes / 1024 / 1024, ratio * 100)
}

// calculateProbeInterval calculates ProbeInterval based on IAT
// IAT가 짧으면 (빈번한 호출) → 짧은 interval로 빠르게 탐색
// IAT가 길면 (드문 호출) → 긴 interval로 안정적으로 탐색
func calculateProbeInterval(iat float64) time.Duration {
	if iat <= 0 {
		// No IAT data available, use default
		return initialProbeInterval
	}
	
	// Convert IAT (seconds) to duration and apply ratio
	// IAT가 짧을수록 (예: 1초) → 짧은 interval (예: 0.3초)
	// IAT가 길수록 (예: 10초) → 긴 interval (예: 3초)
	calculatedInterval := time.Duration(iat * probeIntervalRatio * float64(time.Second))
	
	// Clamp to bounds
	if calculatedInterval < minProbeInterval {
		return minProbeInterval
	}
	if calculatedInterval > maxProbeInterval {
		return maxProbeInterval
	}
	
	return calculatedInterval
}

func setMemMax(containerID string, targetMax int64) {
	path := cgroupPathFor(containerID, "memory.max")
	data := []byte(fmt.Sprintf("%d", targetMax))
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		// log.Printf("Failed to set mem max for container %s: %v", containerID, err)
		log.Printf("Failed to set mem max for container %s: %v", containerID, err)
	}
}