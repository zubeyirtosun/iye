package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
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

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
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

	t, err := tailer.NewTailer(&cfg.Tailer, logger)
	if err != nil {
		logger.Fatal("Failed to create tailer", zap.Error(err))
	}

	if err := t.Start(); err != nil {
		logger.Fatal("Failed to start tailer", zap.Error(err))
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

	// Main processing pipeline
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer t.Stop()
		defer recoverPanic(logger, "pipeline")

		for {
			select {
			case <-ctx.Done():
				return
			case line := <-t.Output():
				processedLine := processLogLine(line, m, metricsCollector, samplingController, buf, logger)
				if processedLine != nil {
					logger.Debug("Log line output",
						zap.String("source", processedLine.Source),
						zap.String("content", processedLine.Content),
					)
				}
			case err := <-t.Errors():
				logger.Error("Tailer error", zap.Error(err))
			}
		}
	}()

	<-ctx.Done()

	// Graceful shutdown: stop tailer first (no new input), then transport, then close buffer
	logger.Info("Shutting down...")

	t.Stop()
	if tr != nil {
		tr.Stop()
	}
	if buf != nil {
		buf.Close()
	}

	wg.Wait()
	logger.Info("İYE stopped gracefully")
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

	// Step 2: Update metrics (always run if collector is available)
	if metricsCollector != nil {
		metricsCollector.ProcessLine(maskedLine)
	}

	// Step 3: Sampling decision
	var shouldSample bool
	if samplingController != nil {
		samplingController.ProcessEvent(maskedLine.Severity, maskedLine.Content, maskedLine.Source)
		shouldSample = samplingController.ShouldSample()
	} else {
		shouldSample = true
	}

	if !shouldSample {
		if metricsCollector != nil {
			metricsCollector.DropLine(maskedLine.Source)
		}
		return nil
	}

	// Step 4: Buffer the line (only if buffer is enabled and available)
	if buf != nil {
		entry := models.LogEntry{
			Timestamp: maskedLine.Timestamp.Format(time.RFC3339Nano),
			Source:    maskedLine.Source,
			Message:   maskedLine.Content,
			Level:     maskedLine.Severity.String(),
			Labels:    make(map[string]string),
		}

		if err := buf.Write(entry); err != nil {
			logger.Error("Failed to write to buffer", zap.Error(err))
		} else if metricsCollector != nil {
			size, err := buf.Size()
			if err == nil {
				metricsCollector.UpdateBufferSize("main", int64(size))
			}
		}
	}

	maskedLine.Sampled = true
	return maskedLine
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