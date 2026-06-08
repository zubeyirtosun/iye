package main

import (
	"testing"
	"time"

	"github.com/iye/iye/internal/masker"
	"github.com/iye/iye/internal/metrics"
	"github.com/iye/iye/internal/sampling"
	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

func TestProcessLogLine_SamplingDisabled(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	m, err := masker.NewMasker(&models.MaskerConfig{Enabled: false}, logger)
	if err != nil {
		t.Fatal(err)
	}
	sc := sampling.NewSamplingController(&models.SamplingConfig{Enabled: false}, logger)

	line := &models.LogLine{
		Timestamp: time.Now(),
		Source:    "test",
		Content:   "test log line",
		Severity:  models.SeverityInfo,
	}

	result := processLogLine(line, m, nil, sc, nil, logger)
	if result == nil {
		t.Fatal("Expected non-nil result when sampling disabled")
	}
	if !result.Sampled {
		t.Error("Expected Sampled=true when sampling disabled")
	}
}

func TestProcessLogLine_MetricsSampledFlagOrder(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	m, err := masker.NewMasker(&models.MaskerConfig{Enabled: false}, logger)
	if err != nil {
		t.Fatal(err)
	}

	metricsCollector, err := metrics.NewMetricsCollector(&models.MetricsConfig{Enabled: false}, logger)
	if err != nil {
		t.Fatal(err)
	}

	// Use 100% error rate to force anomaly → SampleRate=1.0 → always sampled
	sc := sampling.NewSamplingController(&models.SamplingConfig{
		Enabled:         true,
		ErrorThreshold:  0.1,
		WindowSize:      10 * time.Minute,
		CooldownPeriod:  1 * time.Minute,
		MinSampleRate:   0.01,
		MaxSampleRate:   1.0,
		AnomalyWindowMinutes: 5,
	}, logger)

	// Process an error event — ProcessEvent + ShouldSample inside processLogLine
	line := &models.LogLine{
		Timestamp: time.Now(),
		Source:    "test",
		Content:   "error: something went wrong",
		Severity:  models.SeverityError,
		Sampled:   false,
	}

	result := processLogLine(line, m, metricsCollector, sc, nil, logger)
	if result == nil {
		t.Fatal("Expected non-nil result for error line (should be sampled)")
	}
	if !result.Sampled {
		t.Error("Expected Sampled=true after processing error line in anomaly mode")
	}

	// Verify that ProcessLine saw the Sampled=true flag
	stats := metricsCollector.Stats()
	if stats.LinesSampled == 0 {
		t.Error("Expected LinesSampled > 0 — ProcessLine was called after Sampled flag was set")
	}
	if stats.LinesProcessed == 0 {
		t.Error("Expected LinesProcessed > 0")
	}
	if stats.LinesDropped > 0 {
		t.Error("Expected LinesDropped == 0 for sampled line")
	}
}

func TestProcessLogLine_NilMasker(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sc := sampling.NewSamplingController(&models.SamplingConfig{Enabled: false}, logger)

	line := &models.LogLine{
		Timestamp: time.Now(),
		Source:    "test",
		Content:   "test log line",
		Severity:  models.SeverityInfo,
	}

	result := processLogLine(line, nil, nil, sc, nil, logger)
	if result == nil {
		t.Fatal("Expected non-nil result when sampling disabled and no masker")
	}
	if !result.Sampled {
		t.Error("Expected Sampled=true")
	}
}
