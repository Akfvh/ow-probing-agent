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
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			updateProbingStates()
		}
	}()

	httpPort := flag.Int("http-port", 8080, "Port to listen on for HTTP requests")
	webhookPort := flag.Int("webhook-port", 50051, "Port to listen on for Webhook requests")
	bridgeURL := flag.String("bridge-url", "http://172.17.0.1:50051/updateCommits", "URL of the bridge server for commits")

	flag.Parse()

	go startPushingCommits(*bridgeURL)

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

func handleAddContainer(w http.ResponseWriter, r *http.Request) {
	beginTime := time.Now()

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

	// for now, just log the request
	log.Printf("Adding container %s for probing", req.ContainerID)

	if err := startMonitoring(req.ContainerID); err != nil {
		http.Error(w, "Failed to start monitoring container", http.StatusInternalServerError)
		return
	}

	resp := ProbingResponse{
		Status: "success",
		ContainerID: req.ContainerID,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)	

	log.Printf("Took %s to add container %s", time.Since(beginTime), req.ContainerID)

	// log.Printf("Containers: %v", containers)
	// DEBUG, will delete later
	containersMu.RLock()
	log.Printf("Monitoring %d containers", len(containers))
	containersMu.RUnlock()
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

	log.Printf("Removing container %s", req.ContainerID)

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

	// update invocation count for the container
	var invocationCount int64
	var found bool

	containersMu.Lock()
	if container, ok := containers[req.ContainerID]; ok {
		container.StepInvocationCount++ // aggregate to total upon stepping
		invocationCount = container.InvocationCount + container.StepInvocationCount
		found = true
		
		// TODO. decide whether to reset timer on invocation or not
		// if container.State == ProbeProbing {
		// 	container.ProbingStartTime = time.Now()
		// }
	}
	containersMu.Unlock()

	if found {
		log.Printf("Invocation count for container %s: %d", req.ContainerID, invocationCount)
	} else {
		log.Printf("Container %s not found in monitoring map", req.ContainerID)
	}
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

func startMonitoring(containerID string) error {
	initEpoll()

	// examine container specs
	memoryMax, memoryPeak, memoryCur, err := getContainerSpecs(containerID)
	if err != nil {
		log.Printf("Failed to start monitoring container %s: %v", containerID, err)
		return err
	}

	// compute target high
	finalTargetHigh, category := computeTargetHigh(memoryMax, memoryPeak)
	nextTarget := nextProbeTarget(memoryMax, finalTargetHigh, alphaFor(category), minStepBytes)

	// setup psi fd for EPOLL
	psiFD, err := setupPsiFD(containerID)
	if err != nil {
		log.Printf("Failed to setup psi fd for container %s: %v", containerID, err)
		return err
	}

	now := time.Now()

	// put container in monitoring map
	containersMu.Lock()
	container := &ContainerState{
		ContainerID: containerID,
		UserMax: memoryMax,
		CurrentLimit: memoryMax,
		TargetLimit: nextTarget,
		FinalTargetLimit: finalTargetHigh,
		InvocationCount: 0,
		StepInvocationCount: 0,
		Category: category,
		State: ProbeProbing,
		ProbingStartTime: now,
		LastThrottleTime: time.Time{},
		ThrottleCount: 0,
		ProbeInterval: initialProbeInterval,
		LastCommitTime: now,
		psiFD: psiFD,
		lastThrottledLimit: memoryMax,
		consecutiveThrottles: 0,
		committed: false,
	}

	if nextTarget == memoryMax {
		container.State = ProbeIdle
		container.committed = true
		commitsMu.Lock()
		commits = append(commits, ProbeCompleteReport{
			ContainerID: containerID,
			Downsized: false,
			NewLimitMB: memoryMax / 1024 / 1024,
		})
		commitsMu.Unlock()
	}
	containers[containerID] = container
	containersMu.Unlock()

	// add to psi map
	psiMu.Lock()
	fdToContainer[psiFD] = containerID
	psiMu.Unlock()

	// set soft limit
	setMemHigh(containerID, nextTarget)

	// log.Printf("Started monitoring container %s at %s with memory max: %d, memory peak: %d, memory current: %d, target high: %d, category: %s", containerID, now, memoryMax, memoryPeak, memoryCur, targetHigh, category)
	log.Printf("Started monitoring container %s with \nmemory max: %dMB\nmemory peak: %dMB\nmemory current: %dMB\nfinal target high: %dMB\ninitial target high: %dMB\ncategory: %s", containerID, memoryMax / 1024 / 1024, memoryPeak / 1024 / 1024, memoryCur / 1024 / 1024, finalTargetHigh / 1024 / 1024, nextTarget / 1024 / 1024, category)

	return nil
}

func stopMonitoring(containerID string) error {
	// remove from containers map
	containersMu.Lock()
	st, ok := containers[containerID]
	if ok {
		delete(containers, containerID)
	}
	containersMu.Unlock()

	if !ok {
		log.Printf("Container %s not found in monitoring map", containerID)
		return fmt.Errorf("container %s not found in monitoring map", containerID)
	}

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

	containersMu.RLock()
	count := len(containers)
	containersMu.RUnlock()
	log.Printf("Stopped monitoring container %s. %d containers remaining", containerID, count)

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
			// probing in progress
			if now.Sub(container.ProbingStartTime) < container.ProbeInterval ||
			container.StepInvocationCount < minStepInvocationCount {
				continue
			}

			// Probing Complete (step complete)
			container.InvocationCount += container.StepInvocationCount
			container.StepInvocationCount = 0
			container.consecutiveThrottles = 0
			container.CurrentLimit = container.TargetLimit
			container.LastCommitTime = now

			// Probing complete (final target reached)
			if container.CurrentLimit <= container.FinalTargetLimit {
				container.State = ProbeIdle
				container.TargetLimit = container.CurrentLimit
				log.Printf("Probing complete for container %s with memory limit %dMB", container.ContainerID, container.TargetLimit / 1024 / 1024)
				continue
			}

			// Next Probing Step
			alpha := alphaFor(container.Category)
			newTarget := nextProbeTarget(container.CurrentLimit, container.FinalTargetLimit, alpha, minStepBytes)
			container.ProbingStartTime = now

			// can't make further progress. park it.
			if newTarget >= container.CurrentLimit {
				container.State = ProbeIdle
				container.TargetLimit = container.CurrentLimit
				log.Printf("Can't make further progress for container %s with memory limit %dMB", container.ContainerID, container.TargetLimit / 1024 / 1024)
				continue
			}

			container.TargetLimit = newTarget
			container.ProbeInterval = container.ProbeInterval * 2 // exponential backoff

			changes = append(changes, memChange{id: container.ContainerID, limit: newTarget})
			log.Printf("Lowered probing container %s with memory limit %dMB", container.ContainerID, newTarget / 1024 / 1024)


		case ProbeThrottled:
			containersPerState[ProbeThrottled]++
			// current mem.high == userMax (throttled)
			// drain bursts for a while, then decide whether to resume probing
			if now.Sub(container.LastThrottleTime) < effectiveBackoffInterval(container) {
				continue
			}

			container.StepInvocationCount = 0

			// Decide disable probing
			if shouldDisableProbing(container) {
				// a noisy container. disable probing for this session.
				container.committed = true
				container.State = ProbeDisabled
				container.TargetLimit = container.UserMax
				commitsMu.Lock()
				commits = append(commits, ProbeCompleteReport{
					ContainerID: container.ContainerID,
					Downsized: false,
					NewLimitMB: container.UserMax / 1024 / 1024,
				})
				commitsMu.Unlock()
				changes = append(changes, memChange{id: container.ContainerID, limit: container.UserMax})
				log.Printf("Disabled probing for container %s with memory limit %dMB", container.ContainerID, container.UserMax / 1024 / 1024)
				continue
			}

			// Resume probing, more gently
			if container.Category == CategoryLight {
				container.Category = CategoryMedium
			}

			container.TargetLimit = container.CurrentLimit
			container.ProbingStartTime = now
			container.ProbeInterval = initialProbeInterval
			container.consecutiveThrottles = 0

			changes = append(changes, memChange{id: container.ContainerID, limit: container.CurrentLimit})
			log.Printf("Resumed probing container %s with memory limit %dMB", container.ContainerID, container.CurrentLimit / 1024 / 1024)

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
	for _, change := range changes {
		setMemHigh(change.id, change.limit)
		log.Printf("Set memory limit for container %s to %dMB", change.id, change.limit / 1024 / 1024)
	}

	log.Printf("Containers per state: %v", containersPerState)
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