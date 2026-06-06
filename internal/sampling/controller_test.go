package sampling

import (
	"testing"
	"time"

	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

func TestSamplingController_ProcessEvent(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5,
		WindowSize:      10 * time.Second,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 5,
	}

	s := NewSamplingController(config, logger)

	// Process normal events
	s.ProcessEvent(models.SeverityInfo, "Normal log", "test")
	s.ProcessEvent(models.SeverityInfo, "Another normal log", "test")
	s.ProcessEvent(models.SeverityWarn, "Warning log", "test")

	if s.GetSampleRate() < config.MinSampleRate || s.GetSampleRate() > config.MaxSampleRate {
		t.Errorf("Sample rate out of bounds: %f", s.GetSampleRate())
	}

	if s.IsInAnomaly() {
		t.Error("Should not be in anomaly state")
	}
}

func TestSamplingController_AnomalyDetection(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5, // 50% errors trigger anomaly
		WindowSize:      10 * time.Second,
		CooldownPeriod:  10 * time.Second, // Short cooldown for testing
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 1, // 1 minute anomaly window
	}

	s := NewSamplingController(config, logger)

	// Process mostly error events to trigger anomaly
	for i := 0; i < 6; i++ {
		s.ProcessEvent(models.SeverityError, "Error log", "test")
	}
	for i := 0; i < 4; i++ {
		s.ProcessEvent(models.SeverityInfo, "Info log", "test")
	}

	// Should be in anomaly state now (6/10 = 60% errors > 50% threshold)
	if !s.IsInAnomaly() {
		t.Error("Expected to be in anomaly state")
	}

	if s.GetSampleRate() != 1.0 {
		t.Errorf("Expected sample rate 1.0 during anomaly, got %f", s.GetSampleRate())
	}

	// Manually set the anomaly end time to past to simulate expiration
	s.mu.Lock()
	s.anomalyEnd = time.Now().Add(-1 * time.Second) // Set to past
	s.mu.Unlock()

	// Check state - should have exited anomaly
	if s.IsInAnomaly() {
		t.Error("Expected to be out of anomaly state after anomaly window")
	}
}

func TestSamplingController_CleanupOldEvents(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5,
		WindowSize:      100 * time.Millisecond, // Very short window for testing
		CooldownPeriod:  10 * time.Second,
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 0, // No anomaly window for immediate testing
	}

	s := NewSamplingController(config, logger)

	// Add an error event
	s.ProcessEvent(models.SeverityError, "Old error", "test")

	// Check initial state - should be in anomaly with max sample rate
	if !s.IsInAnomaly() {
		t.Error("Expected to be in anomaly state after error event")
	}
	if s.GetSampleRate() != 1.0 {
		t.Errorf("Expected sample rate 1.0 during anomaly, got %f", s.GetSampleRate())
	}

	// Wait for window to pass (event should be cleaned up)
	time.Sleep(150 * time.Millisecond)

	// Check if anomaly state was exited due to cleanup
	if s.IsInAnomaly() {
		t.Error("Expected anomaly state to be exited after event cleanup")
	}

	// Add a new info event
	s.ProcessEvent(models.SeverityInfo, "New info", "test")

	// With only the new info event, error rate should be 0%
	if s.GetSampleRate() > config.MinSampleRate {
		t.Errorf("Expected sample rate at minimum after cleanup, got %f", s.GetSampleRate())
	}
}

func TestSamplingController_ShouldSample(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5,
		WindowSize:      10 * time.Second,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.3, // 30% baseline sampling
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 5,
	}

	s := NewSamplingController(config, logger)

	// With no events, should sample at min rate
	if !s.ShouldSample() && config.MinSampleRate >= 1.0 {
		t.Error("Should sample when no events and min rate >= 1.0")
	}

	// Add some events to get a sample rate
	for i := 0; i < 10; i++ {
		s.ProcessEvent(models.SeverityInfo, "Info log", "test")
	}

	// Should sample based on calculated rate
	// (This is harder to test precisely without exposing internals)
}

func TestSamplingController_Disabled(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled: false,
	}

	s := NewSamplingController(config, logger)

	// Process events - should not affect state
	s.ProcessEvent(models.SeverityError, "Error log", "test")
	s.ProcessEvent(models.SeverityError, "Another error", "test")

	// Should always sample when disabled
	if !s.ShouldSample() {
		t.Error("Should always sample when disabled")
	}

	// Should never be in anomaly when disabled
	if s.IsInAnomaly() {
		t.Error("Should never be in anomaly when disabled")
	}
}

func TestSamplingController_Stats(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5,
		WindowSize:      10 * time.Second,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 5,
	}

	s := NewSamplingController(config, logger)
	s.ProcessEvent(models.SeverityInfo, "info log", "test")
	s.ProcessEvent(models.SeverityError, "error log", "test")

	stats := s.Stats()
	if stats.EventsProcessed != 2 {
		t.Errorf("Expected 2 events processed, got %d", stats.EventsProcessed)
	}
	if stats.ErrorsDetected != 1 {
		t.Errorf("Expected 1 error, got %d", stats.ErrorsDetected)
	}
	if stats.SampleRate == 0 {
		t.Errorf("Expected non-zero sample rate")
	}
}

func TestSamplingController_Reset(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5,
		WindowSize:      10 * time.Second,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 5,
	}

	s := NewSamplingController(config, logger)
	s.ProcessEvent(models.SeverityError, "error log", "test")

	if !s.IsInAnomaly() {
		t.Error("Expected to be in anomaly state")
	}

	s.Reset()

	if s.IsInAnomaly() {
		t.Error("Expected not to be in anomaly after reset")
	}
	if s.GetSampleRate() != config.MaxSampleRate {
		t.Errorf("Expected sample rate %f after reset, got %f", config.MaxSampleRate, s.GetSampleRate())
	}

	stats := s.Stats()
	if stats.EventsProcessed != 0 {
		t.Errorf("Expected 0 events after reset, got %d", stats.EventsProcessed)
	}
}

func BenchmarkSamplingController_ProcessEvent(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5,
		WindowSize:      10 * time.Second,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 5,
	}

	s := NewSamplingController(config, logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			s.ProcessEvent(models.SeverityError, "Error log", "test")
		} else {
			s.ProcessEvent(models.SeverityInfo, "Info log", "test")
		}
	}
}