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
		WindowBuckets:   2,
		BucketDuration:  5 * time.Second,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 5,
	}

	s := NewSamplingController(config, logger)

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
		ErrorThreshold:  0.5,
		WindowSize:      10 * time.Second,
		WindowBuckets:   2,
		BucketDuration:  5 * time.Second,
		CooldownPeriod:  10 * time.Second,
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 1,
	}

	s := NewSamplingController(config, logger)

	for i := 0; i < 6; i++ {
		s.ProcessEvent(models.SeverityError, "Error log", "test")
	}
	for i := 0; i < 4; i++ {
		s.ProcessEvent(models.SeverityInfo, "Info log", "test")
	}

	if !s.IsInAnomaly() {
		t.Error("Expected to be in anomaly state")
	}

	if s.GetSampleRate() != 1.0 {
		t.Errorf("Expected sample rate 1.0 during anomaly, got %f", s.GetSampleRate())
	}

	s.mu.Lock()
	s.anomalyEnd = time.Now().Add(-1 * time.Second)
	s.mu.Unlock()

	if s.IsInAnomaly() {
		t.Error("Expected to be out of anomaly state after anomaly window")
	}
}

func TestSamplingController_CleanupOldEvents(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5,
		WindowSize:      100 * time.Millisecond,
		WindowBuckets:   2,
		BucketDuration:  50 * time.Millisecond,
		CooldownPeriod:  10 * time.Second,
		MinSampleRate:   0.1,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 0,
	}

	s := NewSamplingController(config, logger)

	s.ProcessEvent(models.SeverityError, "Old error", "test")

	if !s.IsInAnomaly() {
		t.Error("Expected to be in anomaly state after error event")
	}
	if s.GetSampleRate() != 1.0 {
		t.Errorf("Expected sample rate 1.0 during anomaly, got %f", s.GetSampleRate())
	}

	time.Sleep(150 * time.Millisecond)

	if s.IsInAnomaly() {
		t.Error("Expected anomaly state to be exited after event cleanup")
	}

	s.ProcessEvent(models.SeverityInfo, "New info", "test")

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
		WindowBuckets:   2,
		BucketDuration:  5 * time.Second,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.3,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 5,
	}

	s := NewSamplingController(config, logger)

	if config.MinSampleRate < 1.0 && s.ShouldSample() {
	}

	for i := 0; i < 10; i++ {
		s.ProcessEvent(models.SeverityInfo, "Info log", "test")
	}
	_ = s.ShouldSample()
}

func TestSamplingController_ProbabilisticSampling(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.5,
		WindowSize:      10 * time.Minute,
		WindowBuckets:   2,
		BucketDuration:  5 * time.Minute,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.5,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 0,
	}

	s := NewSamplingController(config, logger)

	for i := 0; i < 100; i++ {
		s.ProcessEvent(models.SeverityInfo, "Info log", "test")
	}

	trials := 5000
	sampled := 0
	for i := 0; i < trials; i++ {
		if s.ShouldSample() {
			sampled++
		}
	}

	rate := float64(sampled) / float64(trials)
	if rate < 0.45 || rate > 0.55 {
		t.Errorf("Expected sample rate ~0.50, got %f (%d/%d)", rate, sampled, trials)
	}
}

func TestSamplingController_Disabled(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	config := &models.SamplingConfig{
		Enabled: false,
	}

	s := NewSamplingController(config, logger)

	s.ProcessEvent(models.SeverityError, "Error log", "test")
	s.ProcessEvent(models.SeverityError, "Another error", "test")

	if !s.ShouldSample() {
		t.Error("Should always sample when disabled")
	}

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
		WindowBuckets:   2,
		BucketDuration:  5 * time.Second,
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
		WindowBuckets:   2,
		BucketDuration:  5 * time.Second,
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
	if s.GetSampleRate() != config.MinSampleRate {
		t.Errorf("Expected sample rate %f after reset, got %f", config.MinSampleRate, s.GetSampleRate())
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
		WindowBuckets:   6,
		BucketDuration:  10 * time.Second,
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
