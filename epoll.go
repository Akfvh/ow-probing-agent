package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"runtime"
	"golang.org/x/sys/unix"
)

var (
	epollFD int = -1
	epollOnce sync.Once
	fdToContainer = make(map[int]string)
	psiMu sync.RWMutex
)

func initEpoll() {
	epollOnce.Do(func() {
		fd, err := unix.EpollCreate1(0)
		if err != nil {
			log.Fatalf("Failed to create epoll: %v", err)
		}
		epollFD = fd
		go epollLoop()
	})
}

func setupPsiFD(containerID string) (int, error) {
	// trigger configuration
	windowInMs := 500
	thresholdMs := 100
	windowInUs := windowInMs * 1000
	thresholdInUs := thresholdMs * 1000
	trigger := fmt.Sprintf("some %d %d", thresholdInUs, windowInUs)

	f, err := os.OpenFile(cgroupPathFor(containerID, "memory.pressure"), os.O_RDWR, 0)
	if err != nil {
		return -1, err
	}

	// get file descriptor
	fd  := int(f.Fd())

	// set file to non-blocking
	if err := unix.SetNonblock(fd, true); err != nil {
		f.Close()
		return -1, fmt.Errorf("failed to set file to non-blocking: %v", err)
	}

	// setup trigger
	if _, err := f.WriteString(trigger); err != nil {
		return -1, err
	}

	// Rewind file pointer
	//if _, err := unix.Seek(fd, 0, 0); err != nil {
	//	f.Close()
	//	return -1, fmt.Errorf("failed to rewind file pointer: %v", err)
	//}

	event := unix.EpollEvent{
		Events: unix.EPOLLPRI,
		Fd: int32(fd),
	}

	if err := unix.EpollCtl(epollFD, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		f.Close()
		return -1, fmt.Errorf("failed to add psi fd to epoll: %v", err)
	}

	runtime.SetFinalizer(f, nil)

	return fd, nil
}

func epollLoop() {
	events := make([]unix.EpollEvent, 128)

	for {
		n, err := unix.EpollWait(epollFD, events, -1)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			log.Printf("EpollWait failed: %v", err)
			continue
		}

		for i := 0; i < n; i++ {
			ev := events[i]
			fd := int(ev.Fd)

			psiMu.RLock()
			containerID, ok := fdToContainer[fd]
			psiMu.RUnlock()
			if !ok {
				continue // unexpected. maybe removed
			}

			go handleMemoryPressureEvent(containerID, fd, ev)
		}
	}
}

// Quickly revert to original limit upon memory pressure event
func handleMemoryPressureEvent(containerID string, fd int, ev unix.EpollEvent) {
	// drain the event
	buf := make([]byte, 4096)
	for {
		n, err := unix.Read(fd, buf)
		if n <= 0 {
			if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
				break
			}
			if err != nil {
				log.Printf("Failed to read from psi fd for container %s: %v", containerID, err)
			}
			break
		}
	}

	markThrottled(containerID)
}