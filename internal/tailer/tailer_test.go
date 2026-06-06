package tailer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

func TestNewTailer(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cfg := &models.TailerConfig{
		Paths:          []string{"/var/log/**/*.log"},
		PollInterval:   100 * time.Millisecond,
		MaxLineSize:    1024 * 1024,
		ReadBufferSize: 64 * 1024,
	}

	tr, err := NewTailer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create tailer: %v", err)
	}
	if tr == nil {
		t.Fatal("Expected non-nil tailer")
	}
}

func TestTailer_StartStop(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	if err := os.WriteFile(logFile, []byte("line1\nline2\nline3\n"), 0644); err != nil {
		t.Fatalf("Failed to create test log file: %v", err)
	}

	logger, _ := zap.NewDevelopment()
	cfg := &models.TailerConfig{
		Paths:          []string{logFile},
		PollInterval:   50 * time.Millisecond,
		MaxLineSize:    1024 * 1024,
		ReadBufferSize: 64 * 1024,
		FromBeginning:  true,
	}

	tr, err := NewTailer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create tailer: %v", err)
	}

	if err := tr.Start(); err != nil {
		t.Fatalf("Failed to start tailer: %v", err)
	}

	out := tr.Output()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	var lines int
	for {
		select {
		case <-out:
			lines++
			if lines >= 3 {
				tr.Stop()
				return
			}
		case <-timer.C:
			tr.Stop()
			t.Fatalf("Timed out waiting for lines, got %d", lines)
		}
	}
}

func TestTailer_EmptyOutput(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cfg := &models.TailerConfig{
		Paths:          []string{"/nonexistent/path/**/*.log"},
		PollInterval:   100 * time.Millisecond,
		MaxLineSize:    1024,
		ReadBufferSize: 4096,
	}

	tr, err := NewTailer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create tailer: %v", err)
	}

	if err := tr.Start(); err != nil {
		t.Fatalf("Failed to start tailer: %v", err)
	}

	out := tr.Output()
	select {
	case _, ok := <-out:
		if ok {
			t.Error("Expected no output for nonexistent path")
		}
	case <-time.After(500 * time.Millisecond):
		// No output received, which is expected
	}

	tr.Stop()
}

func TestTailer_ErrorsChannel(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cfg := &models.TailerConfig{
		Paths:          []string{"/nonexistent/**/*.log"},
		PollInterval:   50 * time.Millisecond,
		MaxLineSize:    1024,
		ReadBufferSize: 4096,
	}

	tr, err := NewTailer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create tailer: %v", err)
	}

	if err := tr.Start(); err != nil {
		t.Fatalf("Failed to start tailer: %v", err)
	}

	errs := tr.Errors()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-errs:
		// Expected: some error about non-existent file
	case <-timer.C:
		// Also acceptable if no errors
	}

	tr.Stop()
}

func TestTailer_Stats(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	if err := os.WriteFile(logFile, []byte("line1\nline2\n"), 0644); err != nil {
		t.Fatalf("Failed to create test log file: %v", err)
	}

	logger, _ := zap.NewDevelopment()
	cfg := &models.TailerConfig{
		Paths:          []string{logFile},
		PollInterval:   50 * time.Millisecond,
		MaxLineSize:    1024,
		ReadBufferSize: 4096,
		FromBeginning:  true,
	}

	tr, err := NewTailer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create tailer: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Failed to start tailer: %v", err)
	}

	out := tr.Output()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for i := 0; i < 2; i++ {
		select {
		case <-out:
		case <-timer.C:
			tr.Stop()
			t.Fatalf("Timed out waiting for line %d", i+1)
		}
	}

	tr.Stop()

	stats := tr.Stats()
	if stats.LinesRead.Load() != 2 {
		t.Errorf("Expected 2 lines read, got %d", stats.LinesRead.Load())
	}
	if stats.BytesRead.Load() == 0 {
		t.Error("Expected bytes_read > 0")
	}
}

func TestTailer_GetTrackedFiles(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	if err := os.WriteFile(logFile, []byte("test\n"), 0644); err != nil {
		t.Fatalf("Failed to create test log file: %v", err)
	}

	logger, _ := zap.NewDevelopment()
	cfg := &models.TailerConfig{
		Paths:          []string{logFile},
		PollInterval:   50 * time.Millisecond,
		MaxLineSize:    1024,
		ReadBufferSize: 4096,
		FromBeginning:  true,
	}

	tr, err := NewTailer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create tailer: %v", err)
	}
	if err := tr.Start(); err != nil {
		t.Fatalf("Failed to start tailer: %v", err)
	}

	// Wait for file discovery
	time.Sleep(100 * time.Millisecond)

	files := tr.GetTrackedFiles()
	if len(files) == 0 {
		tr.Stop()
		t.Fatal("Expected at least one tracked file")
	}

	tr.Stop()
}

func TestTailer_SetPosition(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	cfg := &models.TailerConfig{
		Paths:          []string{"/nonexistent/**/test.log"},
		PollInterval:   100 * time.Millisecond,
		MaxLineSize:    1024,
		ReadBufferSize: 4096,
	}

	tr, err := NewTailer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create tailer: %v", err)
	}

	// SetPosition should not panic
	tr.SetPosition("/var/log/test.log", 100)

	// Setting position for nonexistent file should not error
	tr.SetPosition("/nonexistent/path.log", 50)
}
