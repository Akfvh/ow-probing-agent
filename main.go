package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"golang.org/x/sys/unix"
)

func main() {
	// add a global ticker to check the status of all containers every 1 second
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			updateProbingStates()
		}
	}()

	httpPort := flag.Int("http-port", 8080, "Port to listen on for HTTP requests")
	webhookPort := flag.Int("webhook-port", 50051, "Port to listen on for Webhook requests")
	bridgeURL := flag.String("bridge-url", "http://172.17.0.1:50051/updateCommits", "URL of the bridge server for commits")
	bridgeBaseURL := flag.String("bridge-base-url", "http://172.17.0.1:50051", "Base URL of the bridge server")

	flag.Parse()

	go startPushingCommits(*bridgeURL)
	
	// Store bridge base URL for probe disabled notifications
	bridgeBaseURLForDisabled = *bridgeBaseURL

	log.Printf(
		"Starting OW Probing Agent on port %d (HTTP) and %d (Webhook)", *httpPort, *webhookPort)

	http.HandleFunc("/health", handleHealth) // healthcheck endpoint
	http.HandleFunc("/containers/add", handleAddContainer) // add container to monitoring batch
	http.HandleFunc("/containers/remove", handleRemoveContainer) // remove container from monitoring batch
	http.HandleFunc("/reclaimed", handleGetReclaimedMemory) // get reclaimed memory snapshot
	http.HandleFunc("/containers/update", handleUpdateProbing) // update probing for container

	addr := fmt.Sprintf(":%d", *httpPort)
	log.Printf("Listening on %s", addr)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(HealthCheckResponse{Status: "ok"})
}

// params: container_id, probe_time
func handleAddContainer(w http.ResponseWriter, r *http.Request) {
	// beginTime := time.Now()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req ProbingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode request body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.ContainerID == "" {
		http.Error(w, "Container ID is required", http.StatusBadRequest)
		return
	}

	// log.Printf("Adding container %s for probing. probe time: %d seconds", req.ContainerID, req.ProbeTime)

	if err := startMonitoring(req); err != nil {
		http.Error(w, "Failed to start monitoring container", http.StatusInternalServerError)
		return
	}

	resp := ProbingResponse{
		Status: "success",
		ContainerID: req.ContainerID,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)	

	// log.Printf("Took %s to add container %s", time.Since(beginTime), req.ContainerID)

	// log.Printf("Containers: %v", containers)
	// DEBUG, will delete later
	// containersMu.RLock()
	// log.Printf("Monitoring %d containers", len(containers))
	// containersMu.RUnlock()
}

func handleRemoveContainer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req ProbingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode request body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.ContainerID == "" {
		http.Error(w, "Container ID is required", http.StatusBadRequest)
		return
	}

	// Log container state before removal for debugging (only if container exists)
	// containersMu.RLock()
	// if container, ok := containers[req.ContainerID]; ok {
	// 	log.Printf("[REMOVE REQUEST] Removing container %s (state: %s, currentLimit: %dMB, userMax: %dMB)", 
	// 		req.ContainerID, container.State, container.CurrentLimit / 1024 / 1024, container.UserMax / 1024 / 1024)
	// }
	// containersMu.RUnlock()

	if err := stopMonitoring(req.ContainerID); err != nil {
		http.Error(w, "Failed to stop monitoring container", http.StatusInternalServerError)
		return
	}

	resp := ProbingResponse{
		Status: "success",
		ContainerID: req.ContainerID,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// Container is touched upon invocation end
func handleUpdateProbing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req ProbingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode request body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// var found bool

	containersMu.Lock()
	if container, ok := containers[req.ContainerID]; ok {
		container.StepInvocationCount++ // aggregate to total upon stepping

		if container.State == ProbeIdle &&
			container.Category != CategoryNoDownsize &&
			container.CurrentLimit > container.FinalTargetLimit {
				// log.Printf("Waking up container %s from idle state", container.ContainerID)

				container.State = ProbeProbing
				container.ProbingStartTime = time.Now()
		} else if container.State == ProbeThrottled && // throttle recovery logic
			container.backoffCount > 0 {
				container.backoffCount--
				// log.Printf("Decreasing backoff count for container %s to %d", container.ContainerID, container.backoffCount)

				// we do resume probing at the main fsm, updateProbingStates()
		}
		// found = true
	}
	containersMu.Unlock()

	// if !found {
	// 	log.Printf("Container %s not found in monitoring map", req.ContainerID)
	// }
}

func handleGetReclaimedMemory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	snapshot := snapshotReclaimedMemory()

	resp := ReclaimedBytesResponse{
		ReclaimedBytes: snapshot.TotalReclaimedMemory,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("Failed to encode snapshot: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func startMonitoring(req ProbingRequest) error {
	containerID := req.ContainerID
	
	// Use ActionStats metrics if available, otherwise use defaults
	coldStartSensitivity := req.ColdStartSensitivity
	if coldStartSensitivity == 0.0 {
		coldStartSensitivity = 1.3 // Default value
	}
	
	// Placeholder for history peak bytes (future use)
	historyPeakBytes := int64(0)
	// if req.HistoryPeakBytes > 0 {
	//	historyPeakBytes = req.HistoryPeakBytes
	// }
	
	// initialize epoll, only once
	initEpoll()

	// examine first invocation.
	memoryMax, memoryPeak, _, err := getContainerSpecs(containerID)
	if err != nil {
		log.Printf("Failed to start monitoring container %s: %v", containerID, err)
		return err
	}

	// default values
	var nextTarget, ssthresh int64
	var category ProbeCategory = CategoryMedium
	var safetyMargin float64 = 1.20 // 20% headroom

	// compute final target
	finalTarget := computeFinalTarget(memoryMax, memoryPeak)
	if historyPeakBytes > finalTarget {
		finalTarget = historyPeakBytes
	}

	// Sensitivity Logics - use ActionStats coldstartSensitivity
	if coldStartSensitivity > 1.0 {
		// High sensitivity
		safetyMargin = 1.70
		// log.Printf("High sensitivity for container %s (sensitivity: %.3f)", containerID, coldStartSensitivity)
	} else if coldStartSensitivity < 0.2 {
		// Low sensitivity
		safetyMargin = 1.45
		category = CategoryLight
		// log.Printf("Low sensitivity for container %s (sensitivity: %.3f)", containerID, coldStartSensitivity)
	} else {
		// log.Printf("Medium sensitivity for container %s (sensitivity: %.3f)", containerID, coldStartSensitivity)
	}
	
	// Calculate ProbeInterval based on IAT
	probeInterval := calculateProbeInterval(req.IAT)
	
	// Log received ActionStats metrics for debugging
	// if req.IAT > 0 || req.CV > 0 {
	// 	log.Printf("ActionStats for container %s: sensitivity=%.3f, iat=%.2fs, cv=%.3f, probeInterval=%.2fs",
	// 		containerID, coldStartSensitivity, req.IAT, req.CV, probeInterval.Seconds())
	// }

	// history available
	if historyPeakBytes > 0 {

		smartStart := int64(float64(historyPeakBytes) * safetyMargin)
		smartStart = (smartStart + 4095) & ^4095

		// ssthresh: always based on finalTarget for consistency
		ssthresh = int64(float64(finalTarget) * ssthreshMarginRatio)
		if ssthresh > memoryMax {
			ssthresh = memoryMax
		}
		if ssthresh < finalTarget {
			ssthresh = finalTarget
		}
		ssthresh = (ssthresh + 4095) & ^4095 // page align

		nextTarget = smartStart
		if nextTarget < finalTarget {
			nextTarget = finalTarget
		}

		// log.Printf("[Smart Init] %s: Hist=%dMB, Sens=%f -> Start=%dMB, Ssthresh=%dMB (based on finalTarget %dMB)",
		// 	containerID, historyPeakBytes>>20, coldStartSensitivity, nextTarget >> 20, ssthresh >> 20, finalTarget >> 20)
	} else {
		// log.Printf("[DEFAULT INIT] final target: %dMB, safety margin: %f", finalTarget>>20, safetyMargin)
		// ssthresh: always based on finalTarget for consistency
		ssthresh = int64(float64(finalTarget) * ssthreshMarginRatio)
		if ssthresh > memoryMax {
			ssthresh = memoryMax
		}
		if ssthresh < finalTarget {
			ssthresh = finalTarget
		}
		ssthresh = (ssthresh + 4095) & ^4095 // page align
		nextTarget, _ = nextProbeTarget(memoryMax, finalTarget, ssthresh, coldStartSensitivity)
		// log.Printf("[Default Init] %s: Hist=0MB, Sens=%f -> Start=%dMB, Ssthresh=%dMB (based on finalTarget %dMB)",
		// 	containerID, coldStartSensitivity, nextTarget >> 20, ssthresh >> 20, finalTarget >> 20)
	}

	psiFD, err := setupPsiFD(containerID)
	if err != nil {
		log.Printf("Failed to setup psi fd for container %s: %v", containerID, err)
		return err
	}

	now := time.Now()
	
	// Calculate ProbeInterval based on IAT
	// IAT가 짧으면 (빈번한 호출) → 짧은 interval로 빠르게 탐색
	// IAT가 길면 (드문 호출) → 긴 interval로 안정적으로 탐색
	probeInterval = calculateProbeInterval(req.IAT)
	
	containersMu.Lock()
	container := &ContainerState{
		ContainerID: containerID,
		UserMax: memoryMax,
		CurrentLimit: memoryMax,
		TargetLimit: nextTarget,
		FinalTargetLimit: finalTarget,
		Ssthresh: ssthresh,
		LastKnownPeak: memoryPeak,
		Sensitivity: coldStartSensitivity,
		IAT: req.IAT,
		CV: req.CV,
		InvocationCount: 0,
		StepInvocationCount: 0,
		Category: category,
		State: ProbeProbing,
		ProbingStartTime: now,
		LastThrottleTime: time.Time{},
		ThrottleCount: 0,
		ProbeInterval: probeInterval,
		LastCommitTime: now,
		psiFD: psiFD,
		lastThrottledLimit: memoryMax,
		consecutiveThrottles: 0,
		LastCommittedLimit: memoryMax,
		ProbeTime: req.ProbeTime,
	}

	if nextTarget == memoryMax {
		container.State = ProbeIdle
	}
	containers[containerID] = container
	containersMu.Unlock()

	// add to psi map
	psiMu.Lock()
	fdToContainer[psiFD] = containerID
	psiMu.Unlock()

	// set soft limit
	setMemHigh(containerID, nextTarget)

	// logging
	// log.Printf(
	// 	"Started monitoring container %s\n"+
	// 	"Memory max: %dMB\n"+
	// 	"Memory peak: %dMB\n"+
	// 	"Final target: %dMB\n"+
	// 	"Ssthresh: %dMB\n"+
	// 	"Next target: %dMB\n",
	// 	containerID, 
	// 	memoryMax / 1024 / 1024, 
	// 	memoryPeak / 1024 / 1024, 
	// 	finalTarget / 1024 / 1024,
	// 	ssthresh / 1024 / 1024,
	// 	nextTarget / 1024 / 1024, 
	// )
	return nil
}

func stopMonitoring(containerID string) error {
	// remove from containers map
	containersMu.Lock()
	st, ok := containers[containerID]
	// var stateBeforeRemoval ProbeState
	if ok {
		// stateBeforeRemoval = st.State
		delete(containers, containerID)
	}
	containersMu.Unlock()

	if !ok {
		// Container already removed or never added - this is normal in some cases
		// (e.g., race condition, container removed before monitoring started, etc.)
		// Don't log as error, just return silently
		return nil
	}

	// restore memory.high to original UserMax (remove soft limit)
	// This is important to ensure container is not left with a restrictive limit
	setMemHigh(containerID, st.UserMax)

	// remove from psi map
	if st.psiFD != -1 { // valid psi fd
		if epollFD != -1 { // valid epoll fd
			if err := unix.EpollCtl(epollFD, unix.EPOLL_CTL_DEL, st.psiFD, nil); err != nil {
				log.Printf("Failed to remove psi fd from epoll for container %s: %v", containerID, err)
			}
		}
		psiMu.Lock()
		delete(fdToContainer, st.psiFD)
		psiMu.Unlock()

		if err := unix.Close(st.psiFD); err != nil {
			log.Printf("Failed to close psi fd for container %s: %v", containerID, err)
		}
	}

	// containersMu.RLock()
	// count := len(containers)
	// containersMu.RUnlock()
	// log.Printf("[STOP MONITORING] Stopped monitoring container %s. %d containers remaining (state was: %s, currentLimit: %dMB, userMax: %dMB)", 
	// 	containerID, count, stateBeforeRemoval, st.CurrentLimit / 1024 / 1024, st.UserMax / 1024 / 1024)

	return nil
}

func getContainerSpecs(containerID string) (int64, int64, int64, error) {
	memoryMax, err := getContainerMemoryMax(containerID)
	if err != nil {
		return -1, -1, -1, err
	}
	memoryPeak, err := getContainerMemoryPeak(containerID)
	if err != nil {
		return -1, -1, -1, err
	}
	memoryCur, err := getContainerMemoryCurrent(containerID)
	if err != nil {
		return -1, -1, -1, err
	}
	return memoryMax, memoryPeak, memoryCur, nil
}

func getContainerMemoryMax(containerID string) (int64, error) {
	// read /sys/fs/cgroup/docker/<containerID>/memory.max
	content, err := os.ReadFile(fmt.Sprintf("/sys/fs/cgroup/docker/%s/memory.max", containerID))
	if err != nil {
		return -1, err
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return -1, err
	}
	return value, nil
}

func getContainerMemoryPeak(containerID string) (int64, error) {
	// read /sys/fs/cgroup/docker/<containerID>/memory.peak
	content, err := os.ReadFile(fmt.Sprintf("/sys/fs/cgroup/docker/%s/memory.peak", containerID))
	if err != nil {
		return -1, err
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return -1, err
	}
	return value, nil
}

func getContainerMemoryCurrent(containerID string) (int64, error) {
	// read /sys/fs/cgroup/docker/<containerID>/memory.current
	content, err := os.ReadFile(fmt.Sprintf("/sys/fs/cgroup/docker/%s/memory.current", containerID))
	if err != nil {
		return -1, err
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return -1, err
	}
	return value, nil
}

// periodically update probing states of all containers we are watching
func updateProbingStates() {
	// tmp, DEBUG
	var containersPerState = make(map[ProbeState]int)

	now := time.Now()
	var changes []memChange

	containersMu.Lock()
	for _, container := range containers {
		switch container.State {
		case ProbeIdle:
			containersPerState[ProbeIdle]++
			maybeCommit(container, now)
			continue
		case ProbeProbing:
			containersPerState[ProbeProbing]++

			// implement intermediate commit if no traffic AND sufficient memory gain
			timeSinceStart := now.Sub(container.ProbingStartTime)
			if timeSinceStart > intermediateCommitNoTrafficDuration && container.StepInvocationCount == 0 {
				// Check memory gain: must have saved enough memory to justify commit
				savedBytes := container.LastCommittedLimit - container.CurrentLimit
				
				// Safety check: if no memory saved or LastCommittedLimit is invalid, skip commit
				if savedBytes <= 0 || container.LastCommittedLimit <= 0 {
					container.State = ProbeIdle
					container.TargetLimit = container.CurrentLimit
					// log.Printf("[PROBE PROBING] Container %s idle (no traffic for 15s, but invalid savedBytes: %dMB, LastCommittedLimit: %dMB)", 
					// 	container.ContainerID, savedBytes / 1024 / 1024, container.LastCommittedLimit / 1024 / 1024)
					continue
				}
				
				savedRatio := float64(savedBytes) / float64(container.LastCommittedLimit)
				
				// Only commit if we have meaningful memory gain
				hasMemoryGain := savedBytes >= intermediateCommitMinSavedBytes || savedRatio >= intermediateCommitMinSavedRatio
				
				if hasMemoryGain {
					container.State = ProbeIdle
					container.TargetLimit = container.CurrentLimit
					// log.Printf("[PROBE PROBING] Intermediate commit for container %s with memory limit %dMB (no traffic for 15s, saved %dMB, %.1f%%)", 
					// 	container.ContainerID, container.CurrentLimit / 1024 / 1024, savedBytes / 1024 / 1024, savedRatio * 100)
					maybeCommit(container, now)
					continue
				} else {
					// Not enough memory gain, just transition to idle without commit
					container.State = ProbeIdle
					container.TargetLimit = container.CurrentLimit
					// log.Printf("[PROBE PROBING] Container %s idle (no traffic for 15s, but insufficient memory gain: %dMB saved, %.1f%%)", 
					// 	container.ContainerID, savedBytes / 1024 / 1024, savedRatio * 100)
					continue
				}
			}

			// probing in progress
			// if now.Sub(container.ProbingStartTime) < container.ProbeInterval ||
			if timeSinceStart < container.ProbeInterval ||
				container.StepInvocationCount < minStepInvocationCount {
				continue
			}

			// Probing Complete (step complete)
			container.InvocationCount += container.StepInvocationCount
			container.StepInvocationCount = 0
			container.consecutiveThrottles = 0
			container.CurrentLimit = container.TargetLimit
			// Note: LastCommitTime is only updated in maybeCommit() when actual commit happens

			// Probing complete (final target reached)
			if container.CurrentLimit <= container.FinalTargetLimit {
				container.State = ProbeIdle
				container.TargetLimit = container.CurrentLimit
				log.Printf("[COMPLETE] Container %s: reached final target %dMB", container.ContainerID, container.TargetLimit / 1024 / 1024)
				continue
			}

			/* Normal Probing Flow */

			// Next Probing Step: limit & mode
			newTarget, mode := nextProbeTarget(container.CurrentLimit,
				container.FinalTargetLimit,
				container.Ssthresh,
				container.Sensitivity,
			)
			container.ProbingStartTime = now

			// can't make further progress. park it.
			if newTarget >= container.CurrentLimit {
				container.State = ProbeIdle
				container.TargetLimit = container.CurrentLimit
				log.Printf("[STUCK] Container %s: can't make progress at %dMB", container.ContainerID, container.TargetLimit / 1024 / 1024)
				continue
			}

			// Adjust ProbeInterval based on phase and IAT
			// Base interval is calculated from IAT, then adjusted by phase
			baseInterval := calculateProbeInterval(container.IAT)
			if mode == "QuickStart" {
				// QuickStart: faster probing (0.5x of base interval)
				container.ProbeInterval = time.Duration(float64(baseInterval) * 0.5)
				// Ensure minimum 500ms for QuickStart
				if container.ProbeInterval < 500*time.Millisecond {
					container.ProbeInterval = 500 * time.Millisecond
				}
			} else {
				// OomAvoidance: accelerate by using shorter interval (0.7x of base)
				// This speeds up convergence while still maintaining some stability
				container.ProbeInterval = time.Duration(float64(baseInterval) * 0.7)
				// Ensure minimum 1s for OomAvoidance (slightly faster than QuickStart minimum)
				if container.ProbeInterval < 1*time.Second {
					container.ProbeInterval = 1 * time.Second
				}
			}

			container.TargetLimit = newTarget

			changes = append(changes, memChange{id: container.ContainerID, limit: newTarget})

			log.Printf("[PROBE] Container %s: %dMB (mode: %s)", container.ContainerID, newTarget / 1024 / 1024, mode)


		case ProbeThrottled:
			containersPerState[ProbeThrottled]++

			// handled at updateProbing()
			if container.backoffCount > 0 {
				continue
			}

			// Decide disable probing
			if shouldDisableProbing(container) {
				// a noisy container. disable probing for this session.
				container.LastCommittedLimit = container.UserMax
				container.TargetLimit = container.UserMax
				commitsMu.Lock()
				commits = append(commits, ProbeCompleteReport{
					ContainerID: container.ContainerID,
					Downsized: false,
					NewLimitBytes: container.UserMax,
				})
				commitsMu.Unlock()
				changes = append(changes, memChange{id: container.ContainerID, limit: container.UserMax})
				
				containerID := container.ContainerID
				reason := getDisableReason(container)
				log.Printf("Disabled probing for container %s with memory limit %dMB, stopping monitoring (reason: %s)", containerID, container.UserMax / 1024 / 1024, reason)
				
				// Note: We'll call stopMonitoring after releasing the lock
				// to avoid deadlock and ensure proper cleanup
				containersMu.Unlock()
				
				// Notify bridge that probing is disabled BEFORE stopping monitoring
				// This ensures invoker gets the notification while container still exists
				notifyProbeDisabled(containerID, reason)
				
				if err := stopMonitoring(containerID); err != nil {
					log.Printf("Failed to stop monitoring disabled container %s: %v", containerID, err)
				}
				containersMu.Lock()
				continue
			}

			// Hybrid resume strategy:
			// 1. First throttle (consecutiveThrottles == 1): Assume spike, resume near throttle limit
			// 2. Consecutive throttles (>= 2): Assume workload shift, check memory.peak and adjust upward
			var resumeLimit int64
			var resumeReason string
			
			if container.consecutiveThrottles == 1 {
				// Option 1: Spike assumption - resume near throttle limit
				if container.lastThrottledLimit > 0 {
					// Start from throttle limit + safety margin (15%), but don't exceed ssthresh
					resumeLimit = int64(float64(container.lastThrottledLimit) * 1.15)
					resumeLimit = (resumeLimit + 4095) & ^4095 // page align
					
					// Don't start higher than ssthresh
					if resumeLimit > container.Ssthresh {
						resumeLimit = container.Ssthresh
					}
					
					// Don't start lower than final target
					if resumeLimit < container.FinalTargetLimit {
						resumeLimit = container.FinalTargetLimit
					}
					resumeReason = "spike_assumption"
				} else {
					// Fallback to ssthresh if lastThrottledLimit not available
					resumeLimit = container.Ssthresh
					resumeReason = "spike_assumption_fallback"
				}
			} else {
				// Option 2: Workload shift assumption - check memory.peak and adjust
				_, memoryPeak, _, err := getContainerSpecs(container.ContainerID)
				if err != nil {
					log.Printf("Failed to read memory.peak for container %s during resume: %v, using ssthresh", container.ContainerID, err)
					resumeLimit = container.Ssthresh
					resumeReason = "workload_shift_fallback"
				} else {
					// Check if peak increased (workload shift)
					if memoryPeak > container.LastKnownPeak && container.LastKnownPeak > 0 {
						// Peak increased: recalculate FinalTargetLimit based on new peak
						newTarget := computeFinalTarget(container.UserMax, memoryPeak)
						container.FinalTargetLimit = newTarget
						container.LastKnownPeak = memoryPeak
						
						// Recalculate ssthresh based on new FinalTargetLimit (they move together)
						newSsthresh := int64(float64(newTarget) * ssthreshMarginRatio)
						if newSsthresh > container.UserMax {
							newSsthresh = container.UserMax
						}
						if newSsthresh < newTarget {
							newSsthresh = newTarget
						}
						newSsthresh = (newSsthresh + 4095) & ^4095 // page align
						container.Ssthresh = newSsthresh
						
						// Resume from new target or ssthresh, whichever is higher
						if newTarget > container.Ssthresh {
							resumeLimit = newTarget
						} else {
							resumeLimit = container.Ssthresh
						}
						resumeReason = fmt.Sprintf("workload_shift_peak_increased_%dMB", memoryPeak>>20)
					} else {
						// Peak unchanged: moderate upward adjustment (use ssthresh)
						resumeLimit = container.Ssthresh
						resumeReason = "workload_shift_peak_unchanged"
					}
				}
			}

			container.TargetLimit = resumeLimit
			container.CurrentLimit = resumeLimit

			container.StepInvocationCount = 0
			container.ProbingStartTime = now
			// Recalculate ProbeInterval based on IAT (may have changed, but typically stable)
			container.ProbeInterval = calculateProbeInterval(container.IAT)
			// Note: consecutiveThrottles is NOT reset here - it's only reset after successful probing step
			// This allows tracking if container throttles again soon after resume

			changes = append(changes, memChange{id: container.ContainerID, limit: container.TargetLimit})
			log.Printf("[RESUMED] Container %s resumed at %dMB (from %dMB, reason: %s)", 
				container.ContainerID, container.TargetLimit / 1024 / 1024, 
				container.lastThrottledLimit / 1024 / 1024, resumeReason)

			container.State = ProbeProbing

		case ProbeDisabled:
			containersPerState[ProbeDisabled]++
			// disabled probing for this session
			continue
		default:
		}
	}
	containersMu.Unlock()

	// apply changes
	// Note: changes are applied after releasing the lock to avoid holding it during I/O.
	// Container may be removed between lock release and setMemHigh, but setMemHigh will
	// fail gracefully if container doesn't exist.
	for _, change := range changes {
		// Verify container still exists before applying change
		containersMu.RLock()
		_, exists := containers[change.id]
		containersMu.RUnlock()
		
		if exists {
			setMemHigh(change.id, change.limit)
			log.Printf("Set memory limit for container %s to %dMB", change.id, change.limit / 1024 / 1024)
		} else {
			log.Printf("Container %s no longer exists, skipping memory limit change", change.id)
		}
	}

	// log.Printf("Containers per state: %v", containersPerState)
}

func snapshotReclaimedMemory() ReclaimedSnapshot{
	now := time.Now()
	var total int64
	var list []ContainerReclaim

	containersMu.RLock()
	for _, c := range containers{
		reclaimed := reclaimedBytes(c)
		if reclaimed <= 0 {
			continue
		}

		total += reclaimed

		list = append(list, ContainerReclaim{
			ContainerID: c.ContainerID,
			UserMax: c.UserMax,
			CurrentLimit: c.CurrentLimit,
			ReclaimedMemory: reclaimed,
			State: c.State,
			Category: c.Category,
		})
	}
	containersMu.RUnlock()
	
	snapshot := ReclaimedSnapshot{
		Timestamp: now,
		TotalReclaimedMemory: total,
		Containers: list,
	}

	log.Printf("snapshot: total %dMB", total / 1024 / 1024)
	return snapshot
}

func reclaimedBytes(c *ContainerState) int64 {
	if c == nil {
		return 0
	}
	reclaimed := c.UserMax - c.CurrentLimit
	if reclaimed < 0 {
		return 0
	}
	return reclaimed
}
