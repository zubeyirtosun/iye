package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/iye/iye/internal/buffer"
	"github.com/iye/iye/internal/config"
	"github.com/iye/iye/internal/masker"
	"github.com/iye/iye/internal/metrics"
	"github.com/iye/iye/internal/sampling"
	"github.com/iye/iye/internal/tailer"
	"github.com/iye/iye/internal/transport"
	"github.com/iye/iye/pkg/models"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type pipelineWorker struct {
	name       string
	tailer     *tailer.Tailer
	masker     *masker.Masker
	sampler    *sampling.SamplingController
	compliance bool
	metrics    *metrics.MetricsCollector
	buffer     *buffer.DiskBuffer
	wg         sync.WaitGroup
}

type pipelineComponents struct {
	tailer    *tailer.Tailer
	masker    *masker.Masker
	sampler   *sampling.SamplingController
	buffer    *buffer.DiskBuffer
	transport *transport.Transport
}

var logEntryPool = sync.Pool{
	New: func() interface{} {
		return &models.LogEntry{
			Labels: make(map[string]string),
		}
	},
}

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "init" {
		os.Exit(initCmd())
	}

	var (
		configPath  string
		showVersion bool
		logLevel    string
	)

	flag.StringVar(&configPath, "config", "", "Path to configuration file")
	flag.StringVar(&configPath, "c", "", "Path to configuration file (shorthand)")
	flag.StringVar(&logLevel, "log-level", "", "Log level (debug, info, warn, error)")
	flag.BoolVar(&showVersion, "version", false, "Show version information")
	flag.BoolVar(&showVersion, "v", false, "Show version information (shorthand)")
	flag.Parse()

	if showVersion {
		fmt.Printf("İYE Log Squeezer & Anomaly Detector\n")
		fmt.Printf("Version: %s\n", version)
		fmt.Printf("Commit: %s\n", commit)
		fmt.Printf("Build Date: %s\n", buildDate)
		os.Exit(0)
	}

	logger, atomicLevel, err := newLogger(logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting İYE",
		zap.String("version", version),
		zap.String("commit", commit),
	)

	cfg, err := config.LoadOrDefault(configPath)
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	if logLevel != "" {
		cfg.LogLevel = logLevel
	}
	atomicLevel.SetLevel(parseLogLevel(cfg.LogLevel))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Initialize components
	m, err := masker.NewMasker(&cfg.Masker, logger.Named("masker"))
	if err != nil {
		logger.Fatal("Failed to create masker", zap.Error(err))
	}

	metricsCollector, err := metrics.NewMetricsCollector(&cfg.Metrics, logger.Named("metrics"))
	if err != nil {
		logger.Fatal("Failed to create metrics collector", zap.Error(err))
	}

	samplingController := sampling.NewSamplingController(&cfg.Sampling, logger.Named("sampling"))

	if metricsCollector != nil {
		samplingController.SetMetricsRecorder(metricsCollector)
	}

	var buf *buffer.DiskBuffer
	if cfg.Buffer.Enabled {
		buf, err = buffer.NewDiskBuffer(&cfg.Buffer, logger.Named("buffer"))
		if err != nil {
			logger.Fatal("Failed to create buffer", zap.Error(err))
		}
	}

	// Initialize transport (reads from buffer and sends to remote endpoint)
	var tr *transport.Transport
	if cfg.Transport.Enabled {
		if buf == nil {
			logger.Fatal("Transport requires buffer to be enabled")
		}
		var metricsProvider transport.MetricsCollector
		if metricsCollector != nil {
			metricsProvider = metricsCollector
		}
		tr, err = transport.NewTransport(&cfg.Transport, logger.Named("transport"), buf, metricsProvider)
		if err != nil {
			logger.Fatal("Failed to create transport", zap.Error(err))
		}
	}

	// Build pipeline workers
	var workers []*pipelineWorker
	if len(cfg.Pipelines) > 0 {
		for _, pc := range cfg.Pipelines {
			w := buildPipelineWorker(ctx, pc, cfg, logger, metricsCollector, buf, samplingController)
			if w == nil {
				continue
			}
			workers = append(workers, w)
			w.start(logger)
		}
	} else {
		// Legacy single pipeline
		tailerCfg := cfg.Tailer
		t, err := tailer.NewTailer(&tailerCfg, logger)
		if err != nil {
			logger.Fatal("Failed to create tailer", zap.Error(err))
		}
		if err := t.Start(); err != nil {
			logger.Fatal("Failed to start tailer", zap.Error(err))
		}

		w := &pipelineWorker{
			name:    "default",
			tailer:  t,
			masker:  m,
			sampler: samplingController,
			metrics: metricsCollector,
			buffer:  buf,
		}
		workers = append(workers, w)
		w.start(logger)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer recoverPanic(logger, "signal handler")
		select {
		case <-sigCh:
			logger.Info("Shutdown signal received")
			cancel()
		case <-ctx.Done():
		}
	}()

	// Start metrics HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer recoverPanic(logger, "metrics server")
		mux := http.NewServeMux()
		mux.Handle(cfg.Metrics.MetricsPath, promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})

		srv := &http.Server{
			Addr:    cfg.Metrics.ListenAddress,
			Handler: mux,
		}

		go func() {
			logger.Info("Starting metrics server", zap.String("address", cfg.Metrics.ListenAddress))
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("Metrics server failed", zap.Error(err))
			}
		}()

		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		srv.Shutdown(shutdownCtx)
	}()

	// Start transport after server but before tailer pipeline
	if tr != nil {
		tr.Start(ctx)
	}

	// Pipeline workers are already started above; wait for them via the outer wg
	for _, w := range workers {
		w.wg.Add(1)
		go func(w *pipelineWorker) {
			defer w.wg.Done()
			defer w.tailer.Stop()
			defer recoverPanic(logger, "pipeline:"+w.name)



			for {
				select {
				case <-ctx.Done():
					return
				case line := <-w.tailer.Output():
					processedLine := processLogLine(line, w.masker, w.metrics, w.sampler, w.buffer, logger)
					if processedLine != nil {
						logger.Debug("Log line output",
							zap.String("pipeline", w.name),
							zap.String("source", processedLine.Source),
						)
					}
				case err := <-w.tailer.Errors():
					logger.Error("Tailer error", zap.String("pipeline", w.name), zap.Error(err))
				}
			}
		}(w)
	}

	<-ctx.Done()

	// Graceful shutdown: stop tailers first (no new input), then transport, then close buffer
	logger.Info("Shutting down...")

	for _, w := range workers {
		w.tailer.Stop()
	}
	if tr != nil {
		tr.Stop()
	}
	if buf != nil {
		buf.Close()
	}

	wg.Wait()
	logger.Info("İYE stopped gracefully")
}

func (w *pipelineWorker) start(logger *zap.Logger) {
	if err := w.tailer.Start(); err != nil {
		logger.Fatal("Failed to start tailer",
			zap.String("pipeline", w.name),
			zap.Error(err),
		)
	}
	logger.Info("Pipeline started",
		zap.String("name", w.name),
		zap.Bool("compliance", w.compliance),
	)
}

func buildPipelineWorker(
	ctx context.Context,
	pc models.PipelineConfig,
	cfg *models.Config,
	logger *zap.Logger,
	metricsCollector *metrics.MetricsCollector,
	buf *buffer.DiskBuffer,
	defaultSampler *sampling.SamplingController,
) *pipelineWorker {
	// Tailer config: clone global, override paths
	tailerCfg := cfg.Tailer
	tailerCfg.Paths = pc.Paths
	t, err := tailer.NewTailer(&tailerCfg, logger.Named("pipeline:"+pc.Name))
	if err != nil {
		logger.Error("Failed to create tailer for pipeline", zap.String("name", pc.Name), zap.Error(err))
		return nil
	}

	// Masker config: clone global, override if specified
	maskerCfg := cfg.Masker
	if pc.Masker != nil {
		maskerCfg = *pc.Masker
	}
	m, err := masker.NewMasker(&maskerCfg, logger.Named("pipeline:"+pc.Name))
	if err != nil {
		logger.Error("Failed to create masker for pipeline", zap.String("name", pc.Name), zap.Error(err))
		return nil
	}

	// Sampler: compliance pipelines skip sampling entirely
	var sampler *sampling.SamplingController
	if pc.Compliance {
		sampler = sampling.NewSamplingController(&models.SamplingConfig{Enabled: false}, logger.Named("pipeline:"+pc.Name))
	} else {
		samplingCfg := cfg.Sampling
		if pc.Sampling != nil {
			samplingCfg = *pc.Sampling
		}
		sampler = sampling.NewSamplingController(&samplingCfg, logger.Named("pipeline:"+pc.Name))
	}

	if metricsCollector != nil {
		sampler.SetMetricsRecorder(metricsCollector)
	}

	return &pipelineWorker{
		name:       pc.Name,
		tailer:     t,
		masker:     m,
		sampler:    sampler,
		compliance: pc.Compliance,
		metrics:    metricsCollector,
		buffer:     buf,
	}
}

func recoverPanic(logger *zap.Logger, component string) {
	if r := recover(); r != nil {
		logger.Error("Panic in component",
			zap.String("component", component),
			zap.Any("panic", r),
			zap.Stack("stack"),
		)
	}
}

func newLogger(level string) (*zap.Logger, zap.AtomicLevel, error) {
	atomicLevel := zap.NewAtomicLevelAt(parseLogLevel(level))
	cfg := zap.NewProductionConfig()
	cfg.Level = atomicLevel
	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.CallerKey = "caller"
	logger, err := cfg.Build()
	return logger, atomicLevel, err
}

func processLogLine(line *models.LogLine, m *masker.Masker, metricsCollector *metrics.MetricsCollector, samplingController *sampling.SamplingController, buf *buffer.DiskBuffer, logger *zap.Logger) *models.LogLine {
	// Step 1: Masking (always run if masker is available)
	maskedLine := line
	if m != nil {
		maskedLine = m.Mask(line)
		if maskedLine == nil {
			return nil
		}
	}

	// Step 2: Sampling decision (must happen before metrics so Sampled flag is correct)
	var shouldSample bool
	if samplingController != nil {
		samplingController.ProcessEvent(maskedLine.Severity, maskedLine.Content, maskedLine.Source)
		shouldSample = samplingController.ShouldSample()
	} else {
		shouldSample = true
	}

	maskedLine.Sampled = shouldSample

	// Step 3: Update metrics (always run if collector is available — reads Sampled flag)
	if metricsCollector != nil {
		metricsCollector.ProcessLine(maskedLine)

		if !shouldSample {
			metricsCollector.DropLine(maskedLine.Source)
		}
	}

	if !shouldSample {
		return nil
	}

	// Step 4: Buffer the line (only if buffer is enabled and available)
	if buf != nil {
		entry := logEntryPool.Get().(*models.LogEntry)
		entry.Timestamp = maskedLine.Timestamp.Format(time.RFC3339Nano)
		entry.Source = maskedLine.Source
		entry.Message = maskedLine.Content
		entry.Level = maskedLine.Severity.String()
		for k := range entry.Labels {
			delete(entry.Labels, k)
		}

		if err := buf.Write(*entry); err != nil {
			logger.Error("Failed to write to buffer", zap.Error(err))
		} else if metricsCollector != nil {
			size, err := buf.Size()
			if err == nil {
				metricsCollector.UpdateBufferSize("main", int64(size))
			}
		}
		logEntryPool.Put(entry)
	}

	return maskedLine
}

func initCmd() int {
	scanner := bufio.NewScanner(os.Stdin)
	def := models.DefaultConfig()

	fmt.Println("İYE Log Squeezer & Anomaly Detector — Configuration Wizard")
	fmt.Println(strings.Repeat("=", 58))
	fmt.Println("Press Enter to accept defaults shown in [brackets].")
	fmt.Println()

	paths := prompt(scanner, "Log file paths (comma-separated)", strings.Join(def.Tailer.Paths, ", "))
	pollMs := prompt(scanner, "Poll interval (ms)", fmt.Sprint(def.Tailer.PollInterval.Milliseconds()))
	maxLineKB := prompt(scanner, "Max line size (KB)", fmt.Sprint(def.Tailer.MaxLineSize/1024))
	readBufKB := prompt(scanner, "Read buffer size (KB)", fmt.Sprint(def.Tailer.ReadBufferSize/1024))

	maskEnabled := prompt(scanner, "Enable PII masking", "yes")
	maskRepl := prompt(scanner, "Mask replacement string", def.Masker.MaskReplacement)

	metricsEnabled := prompt(scanner, "Enable Prometheus metrics", "yes")
	metricsAddr := prompt(scanner, "Metrics listen address", def.Metrics.ListenAddress)

	samplingEnabled := prompt(scanner, "Enable adaptive sampling", "yes")
	minRate := prompt(scanner, "Minimum sample rate (0.0-1.0)", fmt.Sprint(def.Sampling.MinSampleRate))
	maxRate := prompt(scanner, "Maximum sample rate (0.0-1.0)", fmt.Sprint(def.Sampling.MaxSampleRate))

	transportEnabled := prompt(scanner, "Enable remote transport", "no")
	transportEndpoint := prompt(scanner, "Remote endpoint URL", "")
	batchSize := prompt(scanner, "Batch size", fmt.Sprint(def.Transport.BatchSize))

	cfg := models.DefaultConfig()
	cfg.Tailer.Paths = splitPaths(paths)
	cfg.Tailer.PollInterval = parseDurationMs(pollMs, def.Tailer.PollInterval)
	cfg.Tailer.MaxLineSize = parseKB(maxLineKB, def.Tailer.MaxLineSize)
	cfg.Tailer.ReadBufferSize = parseKB(readBufKB, def.Tailer.ReadBufferSize)

	cfg.Masker.Enabled = isYes(maskEnabled)
	if cfg.Masker.Enabled {
		cfg.Masker.MaskReplacement = maskRepl
	}

	cfg.Metrics.Enabled = isYes(metricsEnabled)
	if cfg.Metrics.Enabled {
		cfg.Metrics.ListenAddress = metricsAddr
	}

	cfg.Sampling.Enabled = isYes(samplingEnabled)
	if rate, err := strconv.ParseFloat(minRate, 64); err == nil {
		cfg.Sampling.MinSampleRate = rate
	}
	if rate, err := strconv.ParseFloat(maxRate, 64); err == nil {
		cfg.Sampling.MaxSampleRate = rate
	}

	cfg.Transport.Enabled = isYes(transportEnabled)
	if cfg.Transport.Enabled {
		if transportEndpoint != "" {
			cfg.Transport.Endpoint = transportEndpoint
		}
	} else {
		cfg.Buffer.Enabled = false
		cfg.Transport.Enabled = false
	}
	cfg.Transport.BatchSize = parseInt(batchSize, def.Transport.BatchSize)

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		return 1
	}

	var outPath string
	if len(os.Args) > 2 {
		outPath = os.Args[2]
	} else {
		outPath = "iye.yaml"
	}

	data, err := config.Marshal(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal config: %v\n", err)
		return 1
	}

	if err := os.WriteFile(outPath, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write config: %v\n", err)
		return 1
	}

	fmt.Printf("\nConfiguration written to %s\n", outPath)
	return 0
}

func prompt(scanner *bufio.Scanner, label, defaultValue string) string {
	fmt.Printf("  %-40s [%s]: ", label, defaultValue)
	if !scanner.Scan() {
		return defaultValue
	}
	val := strings.TrimSpace(scanner.Text())
	if val == "" {
		return defaultValue
	}
	return val
}

func splitPaths(s string) []string {
	var paths []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			paths = append(paths, p)
		}
	}
	return paths
}

func parseDurationMs(s string, fallback time.Duration) time.Duration {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return fallback
	}
	return time.Duration(n) * time.Millisecond
}

func parseKB(s string, fallback int) int {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return fallback
	}
	return n * 1024
}

func parseInt(s string, fallback int) int {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func isYes(s string) bool {
	switch strings.ToLower(s) {
	case "yes", "y", "true", "1", "on":
		return true
	default:
		return false
	}
}

func parseLogLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}