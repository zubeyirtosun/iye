package sampling

import (
	"math/rand"
	"sync"
	"time"

	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

const maxBuckets = 12

type MetricsRecorder interface {
	RecordAnomalyEvent(eventType, source string)
	SetSamplingMode(source string, anomalyMode bool)
}

type eventBucket struct {
	errors uint64
	total  uint64
}

type SamplingController struct {
	config          *models.SamplingConfig
	logger          *zap.Logger
	mu              sync.Mutex
	buckets         [maxBuckets]eventBucket
	numBuckets      int
	bucketDuration  time.Duration
	windowStart     time.Time
	inAnomaly       bool
	anomalyEnd      time.Time
	stats           SamplingStats
	metricsRecorder MetricsRecorder
}

func (s *SamplingController) SetMetricsRecorder(m MetricsRecorder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metricsRecorder = m
}

type SamplingStats struct {
	EventsProcessed  uint64
	ErrorsDetected   uint64
	WarningsDetected uint64
	AnomalyTriggered uint64
	AnomalyEnded     uint64
	SampleRate       float64
}

func NewSamplingController(config *models.SamplingConfig, logger *zap.Logger) *SamplingController {
	if !config.Enabled {
		logger.Info("Sampling controller disabled")
		return &SamplingController{
			config:     config,
			logger:     logger.Named("sampling"),
			numBuckets: 1,
			inAnomaly:  false,
		}
	}

	nb := config.WindowBuckets
	if nb <= 0 {
		nb = 6
	}
	if nb > maxBuckets {
		nb = maxBuckets
	}
	bd := config.BucketDuration
	if bd <= 0 {
		bd = 10 * time.Second
	}

	s := &SamplingController{
		config:         config,
		logger:         logger.Named("sampling"),
		numBuckets:     nb,
		bucketDuration: bd,
		windowStart:    time.Now(),
		inAnomaly:      false,
		stats: SamplingStats{
			SampleRate: config.MinSampleRate,
		},
	}

	s.logger.Info("Sampling controller initialized",
		zap.Duration("window_size", config.WindowSize),
		zap.Int("window_buckets", nb),
		zap.Duration("bucket_duration", bd),
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

	now := time.Now()
	s.rotateWindow(now)

	idx := int(now.Sub(s.windowStart) / s.bucketDuration)
	if idx >= s.numBuckets {
		idx = s.numBuckets - 1
	}

	s.buckets[idx].total++
	s.stats.EventsProcessed++

	switch level {
	case models.SeverityError, models.SeverityFatal, models.SeverityPanic:
		s.buckets[idx].errors++
		s.stats.ErrorsDetected++
	case models.SeverityWarn:
		s.stats.WarningsDetected++
	}

	s.evaluateAnomalyState(now)
}

func (s *SamplingController) rotateWindow(now time.Time) {
	windowDuration := time.Duration(s.numBuckets) * s.bucketDuration
	if now.Sub(s.windowStart) < windowDuration {
		return
	}

	for i := 0; i < s.numBuckets; i++ {
		s.buckets[i].total = 0
		s.buckets[i].errors = 0
	}
	s.windowStart = now
}

func (s *SamplingController) evaluateAnomalyState(now time.Time) {
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

	var totalCount, errorCount uint64
	for _, b := range s.buckets[:s.numBuckets] {
		totalCount += b.total
		errorCount += b.errors
	}

	errorRatio := 0.0
	if totalCount > 0 {
		errorRatio = float64(errorCount) / float64(totalCount)
	}

	if s.inAnomaly {
		if totalCount == 0 || errorRatio < s.config.ErrorThreshold {
			s.logger.Info("Anomaly state resolved",
				zap.Uint64("total_events", totalCount),
				zap.Float64("error_ratio", errorRatio),
			)
			s.inAnomaly = false
			s.stats.AnomalyEnded++
		}
		s.updateSampleRate(0.0)
		return
	}

	if errorRatio >= s.config.ErrorThreshold && totalCount > 0 {
		s.logger.Info("Anomaly detected",
			zap.Uint64("error_count", errorCount),
			zap.Uint64("total_count", totalCount),
			zap.Float64("error_ratio", errorRatio),
			zap.Float64("threshold", s.config.ErrorThreshold),
		)
		s.inAnomaly = true
		if s.config.AnomalyWindowMinutes > 0 {
			s.anomalyEnd = now.Add(time.Duration(s.config.AnomalyWindowMinutes) * time.Minute)
		}
		s.stats.AnomalyTriggered++
		s.reportAnomalyStarted()
		s.updateSampleRate(1.0)
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
		s.stats.SampleRate = 1.0
		return
	}

	var totalCount, errorCount uint64
	for _, b := range s.buckets[:s.numBuckets] {
		totalCount += b.total
		errorCount += b.errors
	}

	if totalCount == 0 {
		s.stats.SampleRate = s.config.MinSampleRate
		return
	}

	errorRatio := float64(errorCount) / float64(totalCount)
	s.stats.SampleRate = s.config.MinSampleRate + (errorRatio * (1.0 - s.config.MinSampleRate))
	if s.stats.SampleRate > s.config.MaxSampleRate {
		s.stats.SampleRate = s.config.MaxSampleRate
	}
}

func (s *SamplingController) ShouldSample() bool {
	if !s.config.Enabled {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.rotateWindow(now)
	s.evaluateAnomalyState(now)

	var totalCount uint64
	for _, b := range s.buckets[:s.numBuckets] {
		totalCount += b.total
	}

	if totalCount == 0 {
		return s.config.MinSampleRate >= 1.0
	}

	return s.stats.SampleRate >= 1.0 || rand.Float64() < s.stats.SampleRate
}

func (s *SamplingController) GetSampleRate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.rotateWindow(now)
	s.evaluateAnomalyState(now)
	return s.stats.SampleRate
}

func (s *SamplingController) IsInAnomaly() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.rotateWindow(now)
	s.evaluateAnomalyState(now)
	return s.inAnomaly
}

func (s *SamplingController) Stats() SamplingStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *SamplingController) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windowStart = time.Now()
	s.inAnomaly = false
	s.anomalyEnd = time.Time{}
	s.stats = SamplingStats{}
	s.stats.SampleRate = s.config.MinSampleRate
	for i := 0; i < s.numBuckets; i++ {
		s.buckets[i].total = 0
		s.buckets[i].errors = 0
	}
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
