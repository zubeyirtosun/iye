package sampling

import (
	"container/list"
	"math/rand"
	"sync"
	"time"

	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

type EventType int

const (
	EventTypeError EventType = iota
	EventTypeWarning
	EventTypeInfo
	EventTypeDebug
)

// MetricsRecorder allows the sampling controller to report anomaly events
// to an external metrics system without creating a dependency cycle.
type MetricsRecorder interface {
	RecordAnomalyEvent(eventType, source string)
	SetSamplingMode(source string, anomalyMode bool)
}

type LogEvent struct {
	Timestamp time.Time
	Level     models.SeverityLevel
	Message   string
	Source    string
}

type SamplingController struct {
	config         *models.SamplingConfig
	logger         *zap.Logger
	mu             sync.RWMutex
	eventQueue     *list.List
	windowStart    time.Time
	inAnomaly      bool
	anomalyEnd     time.Time
	stats          SamplingStats
	metricsRecorder MetricsRecorder
}

func (s *SamplingController) SetMetricsRecorder(m MetricsRecorder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metricsRecorder = m
}

type SamplingStats struct {
	EventsProcessed   uint64
	ErrorsDetected    uint64
	WarningsDetected  uint64
	AnomalyTriggered  uint64
	AnomalyEnded      uint64
	SampleRate        float64
}

func NewSamplingController(config *models.SamplingConfig, logger *zap.Logger) *SamplingController {
	if !config.Enabled {
		logger.Info("Sampling controller disabled")
		return &SamplingController{
			config:     config,
			logger:     logger.Named("sampling"),
			eventQueue: list.New(),
			inAnomaly:  false,
		}
	}

	s := &SamplingController{
		config:     config,
		logger:     logger.Named("sampling"),
		eventQueue: list.New(),
		windowStart: time.Now(),
		inAnomaly:   false,
		stats: SamplingStats{
			SampleRate: config.MinSampleRate,
		},
	}

	s.logger.Info("Sampling controller initialized",
		zap.Duration("window_size", config.WindowSize),
		zap.Duration("cooldown_period", config.CooldownPeriod),
		zap.Float64("error_threshold", config.ErrorThreshold),
		zap.Int("anomaly_window_minutes", config.AnomalyWindowMinutes),
	)

	return s
}

func (s *SamplingController) ProcessEvent(level models.SeverityLevel, message, source string) {
	if !s.config.Enabled {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	event := LogEvent{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Source:    source,
	}

	s.eventQueue.PushBack(event)
	s.stats.EventsProcessed++

	switch level {
	case models.SeverityError, models.SeverityFatal, models.SeverityPanic:
		s.stats.ErrorsDetected++
	case models.SeverityWarn:
		s.stats.WarningsDetected++
	}

	s.cleanupOldEvents()
	s.evaluateAnomalyState()
}

func (s *SamplingController) cleanupOldEvents() {
	cutoff := time.Now().Add(-s.config.WindowSize)

	for e := s.eventQueue.Front(); e != nil; {
		next := e.Next()
		if ev, ok := e.Value.(LogEvent); ok && ev.Timestamp.Before(cutoff) {
			s.eventQueue.Remove(e)
		}
		e = next
	}
}

func (s *SamplingController) evaluateAnomalyState() {
	now := time.Now()

	if s.inAnomaly && s.config.AnomalyWindowMinutes > 0 && now.After(s.anomalyEnd) {
		s.logger.Info("Anomaly period ended",
			zap.Duration("duration", now.Sub(s.anomalyEnd.Add(-time.Duration(s.config.AnomalyWindowMinutes)*time.Minute))),
		)
		s.inAnomaly = false
		s.stats.AnomalyEnded++
		s.reportAnomalyEnded()
		s.updateSampleRate(0.0)
		return
	}

	errorCount := 0
	totalCount := s.eventQueue.Len()

	for e := s.eventQueue.Front(); e != nil; e = e.Next() {
		if ev, ok := e.Value.(LogEvent); ok {
			if ev.Level == models.SeverityError || ev.Level == models.SeverityFatal || ev.Level == models.SeverityPanic {
				errorCount++
			}
		}
	}

	errorRatio := 0.0
	if totalCount > 0 {
		errorRatio = float64(errorCount) / float64(totalCount)
	}

	if s.inAnomaly {
		// If queue is empty or error ratio dropped below threshold, exit anomaly
		if totalCount == 0 || errorRatio < s.config.ErrorThreshold {
			s.logger.Info("Anomaly state resolved",
				zap.Int("total_events", totalCount),
				zap.Float64("error_ratio", errorRatio),
			)
			s.inAnomaly = false
			s.stats.AnomalyEnded++
		}
		s.updateSampleRate(0.0)
		return
	}

	if errorRatio >= s.config.ErrorThreshold {
		s.logger.Info("Anomaly detected",
			zap.Int("error_count", errorCount),
			zap.Int("total_count", totalCount),
			zap.Float64("error_ratio", errorRatio),
			zap.Float64("threshold", s.config.ErrorThreshold),
		)
		s.inAnomaly = true
		if s.config.AnomalyWindowMinutes > 0 {
			s.anomalyEnd = now.Add(time.Duration(s.config.AnomalyWindowMinutes) * time.Minute)
		}
		s.stats.AnomalyTriggered++
		s.reportAnomalyStarted()
		s.updateSampleRate(1.0) // Full sampling when anomaly starts
	} else {
		s.updateSampleRate(0.0)
	}
}

func (s *SamplingController) updateSampleRate(forceRate float64) {
	if forceRate > 0 {
		s.stats.SampleRate = forceRate
		return
	}

	if s.inAnomaly {
		// During anomaly, maintain full sampling
		// Rate will be recalculated when anomaly state is exited
		s.stats.SampleRate = 1.0
		return
	}

	if s.eventQueue.Len() == 0 {
		s.stats.SampleRate = s.config.MinSampleRate
		return
	}

	// Calculate dynamic sample rate based on error ratio
	errorCount := 0
	for e := s.eventQueue.Front(); e != nil; e = e.Next() {
		if ev, ok := e.Value.(LogEvent); ok {
			if ev.Level == models.SeverityError || ev.Level == models.SeverityFatal || ev.Level == models.SeverityPanic {
				errorCount++
			}
		}
	}

	errorRatio := 0.0
	if s.eventQueue.Len() > 0 {
		errorRatio = float64(errorCount) / float64(s.eventQueue.Len())
	}

	s.stats.SampleRate = s.config.MinSampleRate + (errorRatio * (1.0 - s.config.MinSampleRate))
	if s.stats.SampleRate > s.config.MaxSampleRate {
		s.stats.SampleRate = s.config.MaxSampleRate
	}
}

func (s *SamplingController) ShouldSample() bool {
	if !s.config.Enabled {
		return true // If disabled, sample everything
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkAnomalyState(time.Now())

	if s.eventQueue.Len() == 0 {
		return s.config.MinSampleRate >= 1.0
	}

	return s.stats.SampleRate >= 1.0 || rand.Float64() < s.stats.SampleRate
}

func (s *SamplingController) GetSampleRate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkAnomalyState(time.Now())
	return s.stats.SampleRate
}

func (s *SamplingController) IsInAnomaly() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkAnomalyState(time.Now())
	return s.inAnomaly
}

func (s *SamplingController) checkAnomalyState(now time.Time) {
	if !s.inAnomaly {
		return
	}

	// Clean up old events first
	s.cleanupOldEvents()

	// Re-evaluate based on current queue state
	s.evaluateAnomalyState()
}

func (s *SamplingController) Stats() SamplingStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func (s *SamplingController) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventQueue.Init()
	s.windowStart = time.Now()
	s.inAnomaly = false
	s.anomalyEnd = time.Time{}
	s.stats = SamplingStats{}
	s.stats.SampleRate = s.config.MinSampleRate
	s.logger.Info("Sampling controller reset")
}

func (s *SamplingController) reportAnomalyStarted() {
	if s.metricsRecorder == nil {
		return
	}
	s.metricsRecorder.RecordAnomalyEvent("anomaly_started", "global")
	s.metricsRecorder.SetSamplingMode("global", true)
}

func (s *SamplingController) reportAnomalyEnded() {
	if s.metricsRecorder == nil {
		return
	}
	s.metricsRecorder.RecordAnomalyEvent("anomaly_ended", "global")
	s.metricsRecorder.SetSamplingMode("global", false)
}