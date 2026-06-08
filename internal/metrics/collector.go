package metrics

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iye/iye/pkg/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	logLinesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_log_lines_total",
			Help: "Total number of log lines processed",
		},
		[]string{"source", "level"},
	)

	logBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_log_bytes_total",
			Help: "Total bytes of log lines processed",
		},
		[]string{"source"},
	)

	logLinesMasked = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_log_lines_masked_total",
			Help: "Total number of log lines with masked content",
		},
		[]string{"source"},
	)

	logLinesSampled = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_log_lines_sampled_total",
			Help: "Total number of log lines sampled for output",
		},
		[]string{"source", "mode"},
	)

	logLinesDropped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_log_lines_dropped_total",
			Help: "Total number of log lines dropped by sampling",
		},
		[]string{"source"},
	)

	logProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "iye_log_processing_duration_seconds",
			Help:    "Time spent processing log lines",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1},
		},
		[]string{"stage"},
	)

	anomalyEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_anomaly_events_total",
			Help: "Total number of anomaly detection events",
		},
		[]string{"type", "source"},
	)

	currentSamplingMode = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "iye_current_sampling_mode",
			Help: "Current sampling mode (0=normal, 1=anomaly)",
		},
		[]string{"source"},
	)

	bufferSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "iye_buffer_size_bytes",
			Help: "Current size of local buffer in bytes",
		},
		[]string{"buffer"},
	)

	bufferDropped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_buffer_dropped_total",
			Help: "Total number of log lines dropped due to buffer full",
		},
		[]string{"buffer"},
	)

	customPatterns = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_custom_pattern_matches_total",
			Help: "Total matches for custom regex patterns",
		},
		[]string{"pattern", "source"},
	)
)

type PatternMatcher struct {
	name    string
	regex   *regexp.Regexp
	counter *prometheus.CounterVec
}

type CollectorStats struct {
	LinesProcessed  uint64
	BytesProcessed  uint64
	PatternsMatched uint64
	LinesMasked     uint64
	LinesSampled    uint64
	LinesDropped    uint64
	Errors          uint64
}

type MetricsCollector struct {
	config          *models.MetricsConfig
	logger          *zap.Logger
	server          *http.Server
	patternMu       sync.RWMutex
	customPatterns  map[string]*PatternMatcher
	sourceLabels    map[string]map[string]string
	samplingMode    map[string]bool
	linesProcessed  atomic.Uint64
	bytesProcessed  atomic.Uint64
	patternsMatched atomic.Uint64
	linesMasked     atomic.Uint64
	linesSampled    atomic.Uint64
	linesDropped    atomic.Uint64
	errors          atomic.Uint64
}

func NewMetricsCollector(config *models.MetricsConfig, logger *zap.Logger) (*MetricsCollector, error) {
	if !config.Enabled {
		logger.Info("Metrics collector disabled")
		return &MetricsCollector{
			config:         config,
			logger:         logger.Named("metrics"),
			customPatterns: make(map[string]*PatternMatcher),
			sourceLabels:   make(map[string]map[string]string),
			samplingMode:   make(map[string]bool),
		}, nil
	}

	m := &MetricsCollector{
		config:         config,
		logger:         logger.Named("metrics"),
		customPatterns: make(map[string]*PatternMatcher),
		sourceLabels:   make(map[string]map[string]string),
		samplingMode:   make(map[string]bool),
	}

	mux := http.NewServeMux()
	mux.Handle(config.MetricsPath, promhttp.Handler())
	mux.HandleFunc("/healthz", m.healthHandler)
	mux.HandleFunc("/readyz", m.readyHandler)

	server := &http.Server{
		Addr:         config.ListenAddress,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	m.server = server

	for _, cm := range config.CustomMetrics {
		if err := m.AddCustomPattern(cm.Name, cm.Pattern); err != nil {
			return nil, fmt.Errorf("register custom metric %q: %w", cm.Name, err)
		}
	}

	logger.Info("Metrics collector initialized",
		zap.String("listen_address", config.ListenAddress),
		zap.String("metrics_path", config.MetricsPath),
		zap.Int("custom_metrics", len(config.CustomMetrics)),
	)

	return m, nil
}

func (m *MetricsCollector) Start(ctx context.Context) error {
	if !m.config.Enabled || m.server == nil {
		return nil
	}

	go func() {
		m.logger.Info("Starting metrics HTTP server", zap.String("address", m.config.ListenAddress))
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Error("Metrics server error", zap.Error(err))
			m.errors.Add(1)
		}
	}()

	go func() {
		<-ctx.Done()
		m.Stop()
	}()

	return nil
}

func (m *MetricsCollector) Stop() error {
	if m.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m.logger.Info("Stopping metrics HTTP server")
	return m.server.Shutdown(ctx)
}

func (m *MetricsCollector) ProcessLine(line *models.LogLine) {
	start := time.Now()
	defer func() {
		logProcessingDuration.WithLabelValues("process_line").Observe(time.Since(start).Seconds())
	}()

	if line == nil {
		return
	}

	source := line.Source
	if source == "" {
		source = "unknown"
	}

	level := line.Severity.String()
	if level == "unknown" {
		level = m.inferSeverity(line.Content)
	}

	logLinesTotal.WithLabelValues(source, level).Inc()
	logBytesTotal.WithLabelValues(source).Add(float64(len(line.Content)))
	m.linesProcessed.Add(1)
	m.bytesProcessed.Add(uint64(len(line.Content)))

	if line.Masked {
		logLinesMasked.WithLabelValues(source).Inc()
		m.linesMasked.Add(1)
	}

	if line.Sampled {
		mode := "normal"
		if m.isAnomalyMode(source) {
			mode = "anomaly"
		}
		logLinesSampled.WithLabelValues(source, mode).Inc()
		m.linesSampled.Add(1)
	}

	m.matchCustomPatterns(line)
}

func (m *MetricsCollector) inferSeverity(content string) string {
	contentLower := strings.ToLower(content)
	
	switch {
	case strings.Contains(contentLower, "panic") || strings.Contains(contentLower, "fatal"):
		return "fatal"
	case strings.Contains(contentLower, "error") || strings.Contains(contentLower, "exception") || strings.Contains(contentLower, "fail"):
		return "error"
	case strings.Contains(contentLower, "warn") || strings.Contains(contentLower, "warning"):
		return "warn"
	case strings.Contains(contentLower, "debug"):
		return "debug"
	case strings.Contains(contentLower, "info"):
		return "info"
	default:
		return "info"
	}
}

func (m *MetricsCollector) matchCustomPatterns(line *models.LogLine) {
	m.patternMu.RLock()
	patterns := make([]*PatternMatcher, 0, len(m.customPatterns))
	for _, p := range m.customPatterns {
		patterns = append(patterns, p)
	}
	m.patternMu.RUnlock()

	source := line.Source
	if source == "" {
		source = "unknown"
	}

	for _, p := range patterns {
		if p.regex.MatchString(line.Content) {
			m.patternsMatched.Add(1)
			if p.counter != nil {
				p.counter.WithLabelValues(p.name, source).Inc()
			} else {
				customPatterns.WithLabelValues(p.name, source).Inc()
			}
		}
	}
}

func (m *MetricsCollector) AddCustomPattern(name, pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %w", err)
	}

	counter := promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "iye_custom_" + sanitizeMetricName(name) + "_matches_total",
			Help: "Matches for custom pattern: " + name,
		},
		[]string{"pattern", "source"},
	)

	m.patternMu.Lock()
	defer m.patternMu.Unlock()

	m.customPatterns[name] = &PatternMatcher{
		name:    name,
		regex:   re,
		counter: counter,
	}

	m.logger.Info("Added custom metric pattern", zap.String("name", name), zap.String("pattern", pattern))
	return nil
}

func (m *MetricsCollector) RemoveCustomPattern(name string) bool {
	m.patternMu.Lock()
	defer m.patternMu.Unlock()

	if _, exists := m.customPatterns[name]; exists {
		delete(m.customPatterns, name)
		m.logger.Info("Removed custom metric pattern", zap.String("name", name))
		return true
	}
	return false
}

func (m *MetricsCollector) SetSourceLabels(source string, labels map[string]string) {
	m.patternMu.Lock()
	defer m.patternMu.Unlock()
	m.sourceLabels[source] = labels
}

func (m *MetricsCollector) RecordAnomalyEvent(eventType, source string) {
	anomalyEvents.WithLabelValues(eventType, source).Inc()
}

func (m *MetricsCollector) UpdateBufferSize(bufferName string, sizeBytes int64) {
	bufferSize.WithLabelValues(bufferName).Set(float64(sizeBytes))
}

func (m *MetricsCollector) RecordBufferDropped(bufferName string, count int) {
	bufferDropped.WithLabelValues(bufferName).Add(float64(count))
}

// DropLine records a log line that was dropped by sampling
func (m *MetricsCollector) DropLine(source string) {
	m.linesDropped.Add(1)
	logLinesDropped.WithLabelValues(source).Inc()
}

func (m *MetricsCollector) isAnomalyMode(source string) bool {
	m.patternMu.RLock()
	defer m.patternMu.RUnlock()
	return m.samplingMode[source]
}

func (m *MetricsCollector) SetSamplingMode(source string, anomalyMode bool) {
	m.patternMu.Lock()
	defer m.patternMu.Unlock()
	m.samplingMode[source] = anomalyMode
	
	val := 0.0
	if anomalyMode {
		val = 1.0
	}
	currentSamplingMode.WithLabelValues(source).Set(val)
}

func (m *MetricsCollector) Stats() CollectorStats {
	return CollectorStats{
		LinesProcessed:  m.linesProcessed.Load(),
		BytesProcessed:  m.bytesProcessed.Load(),
		PatternsMatched: m.patternsMatched.Load(),
		LinesMasked:     m.linesMasked.Load(),
		LinesSampled:    m.linesSampled.Load(),
		LinesDropped:    m.linesDropped.Load(),
		Errors:          m.errors.Load(),
	}
}

func (m *MetricsCollector) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (m *MetricsCollector) readyHandler(w http.ResponseWriter, r *http.Request) {
	if m.server == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Not Ready"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ready"))
}

func sanitizeMetricName(name string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	return re.ReplaceAllString(name, "_")
}

func (m *MetricsCollector) ParseLogLevel(content string) models.SeverityLevel {
	contentLower := strings.ToLower(content)
	
	switch {
	case strings.Contains(contentLower, "panic"):
		return models.SeverityPanic
	case strings.Contains(contentLower, "fatal"):
		return models.SeverityFatal
	case strings.Contains(contentLower, "error") || strings.Contains(contentLower, "exception"):
		return models.SeverityError
	case strings.Contains(contentLower, "warn") || strings.Contains(contentLower, "warning"):
		return models.SeverityWarn
	case strings.Contains(contentLower, "debug"):
		return models.SeverityDebug
	default:
		return models.SeverityInfo
	}
}

func (m *MetricsCollector) ExtractFields(content string) map[string]string {
	fields := make(map[string]string)
	
	keyValueRegex := regexp.MustCompile(`(\w+)\s*[=:]\s*["']?([^"'\s]+)["']?`)
	matches := keyValueRegex.FindAllStringSubmatch(content, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			key := strings.ToLower(match[1])
			if len(key) > 0 && len(key) < 64 {
				fields[key] = match[2]
			}
		}
	}
	
	return fields
}

func (m *MetricsCollector) ExtractMetricsFromJSON(content string) map[string]float64 {
	metrics := make(map[string]float64)
	
	numberRegex := regexp.MustCompile(`"(\w+)"\s*:\s*([\d.+-]+)`)
	matches := numberRegex.FindAllStringSubmatch(content, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			key := match[1]
			if val, err := strconv.ParseFloat(match[2], 64); err == nil {
				metrics[key] = val
			}
		}
	}
	
	return metrics
}