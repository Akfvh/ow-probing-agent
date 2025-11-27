package main

import (
	"testing"
)

func TestComputeTargetHigh(t *testing.T) {
	// Constants from utilities.go (replicated for verification against current behavior)
	// heavyUsageRatio = 0.85 (but code uses 0.80)
	// mediumUsageRatio = 0.50

	tests := []struct {
		name           string
		userMax        int64
		peak           int64
		expectedTarget int64 // approximated
		expectedCat    ProbeCategory
	}{
		{
			name:        "Heavy usage (>80%)",
			userMax:     100 * 1024 * 1024,
			peak:        85 * 1024 * 1024,
			expectedCat: CategoryNoDownsize,
		},
		{
			name:        "Medium usage (50-80%)",
			userMax:     100 * 1024 * 1024,
			peak:        60 * 1024 * 1024, // 60%
			expectedCat: CategoryMedium,
		},
		{
			name:        "Light usage (20-50%)",
			userMax:     100 * 1024 * 1024,
			peak:        30 * 1024 * 1024, // 30%
			expectedCat: CategoryLight,
		},
		{
			name:        "Ultra light usage (<20%)",
			userMax:     100 * 1024 * 1024,
			peak:        10 * 1024 * 1024, // 10%
			expectedCat: CategoryHeavy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, cat := computeTargetHigh(tt.userMax, tt.peak)
			if cat != tt.expectedCat {
				t.Errorf("expected category %v, got %v", tt.expectedCat, cat)
			}
			if tt.expectedCat == CategoryNoDownsize {
				if target != tt.userMax {
					t.Errorf("expected target %v, got %v", tt.userMax, target)
				}
			} else {
				// verify clamping/logic roughly
				if target > tt.userMax {
					t.Errorf("target %v is greater than userMax %v", target, tt.userMax)
				}
			}
		})
	}
}

func TestNextProbeTarget(t *testing.T) {
	tests := []struct {
		name        string
		current     int64
		finalTarget int64
		alpha       float64
		minStep     int64
		expected    int64
	}{
		{
			name:        "Already at target",
			current:     100,
			finalTarget: 100,
			alpha:       0.5,
			minStep:     10,
			expected:    100,
		},
		{
			name:        "Large step",
			current:     200 * 1024 * 1024,
			finalTarget: 100 * 1024 * 1024,
			alpha:       0.5,
			minStep:     10 * 1024 * 1024,
			expected:    150 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := nextProbeTarget(tt.current, tt.finalTarget, tt.alpha, tt.minStep)
			if val < tt.finalTarget {
				t.Errorf("next target %v is less than final target %v", val, tt.finalTarget)
			}
			if val > tt.current && tt.current > tt.finalTarget {
				t.Errorf("next target %v increased from current %v", val, tt.current)
			}
		})
	}
}

func TestEffectiveBackoffInterval(t *testing.T) {
	container := &ContainerState{
		ThrottleCount: 1,
	}

	// First throttle
	d := effectiveBackoffInterval(container)
	if d != backoffInterval {
		t.Errorf("expected %v, got %v", backoffInterval, d)
	}

	// Second throttle
	container.ThrottleCount = 2
	d = effectiveBackoffInterval(container)
	if d != 2*backoffInterval {
		t.Errorf("expected %v, got %v", 2*backoffInterval, d)
	}
}
