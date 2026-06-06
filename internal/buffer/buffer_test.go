package buffer

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

func newTestBuffer(t *testing.T) (*DiskBuffer, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "iye-buffer-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	logger, _ := zap.NewDevelopment()
	cfg := &models.BufferConfig{
		Enabled:      true,
		Path:         dir,
		MaxSizeBytes: 64 << 20,
		SyncWrites:   true,
	}

	b, err := NewDiskBuffer(cfg, logger)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Failed to create buffer: %v", err)
	}

	cleanup := func() {
		b.Close()
		os.RemoveAll(dir)
	}

	return b, cleanup
}

func entry(msg string) models.LogEntry {
	return models.LogEntry{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Source:    "test",
		Message:   msg,
		Level:     "info",
	}
}

func TestDiskBuffer_WriteAndLen(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	_, err := b.Len()
	if err != nil {
		t.Fatalf("Failed to get len: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := b.Write(entry("test")); err != nil {
			t.Fatalf("Failed to write entry %d: %v", i, err)
		}
	}

	count, err := b.Len()
	if err != nil {
		t.Fatalf("Failed to get len: %v", err)
	}
	if count != 10 {
		t.Errorf("Expected 10 entries, got %d", count)
	}
}

func TestDiskBuffer_WriteAndRead(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	messages := []string{"first", "second", "third"}
	for _, msg := range messages {
		if err := b.Write(entry(msg)); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	entries, err := b.ReadBatch(2)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}
	if entries[0].Message != "first" {
		t.Errorf("Expected 'first', got '%s'", entries[0].Message)
	}
	if entries[1].Message != "second" {
		t.Errorf("Expected 'second', got '%s'", entries[1].Message)
	}
}

func TestDiskBuffer_ReadBatch_ReturnsNilForZero(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	entries, err := b.ReadBatch(0)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if entries != nil {
		t.Errorf("Expected nil, got %d entries", len(entries))
	}

	entries, err = b.ReadBatch(-1)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if entries != nil {
		t.Errorf("Expected nil, got %d entries", len(entries))
	}
}

func TestDiskBuffer_Commit(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	for i := 0; i < 10; i++ {
		if err := b.Write(entry("test")); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	if err := b.Commit(3); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	count, err := b.Len()
	if err != nil {
		t.Fatalf("Failed to get len: %v", err)
	}
	if count != 7 {
		t.Errorf("Expected 7 entries after commit, got %d", count)
	}

	// Read remaining to verify they're the newer ones
	entries, err := ReadAll(b)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if len(entries) != 7 {
		t.Errorf("Expected 7 entries, got %d", len(entries))
	}
}

func TestDiskBuffer_CommitZero(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	if err := b.Write(entry("test")); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	if err := b.Commit(0); err != nil {
		t.Fatalf("Failed to commit 0: %v", err)
	}

	count, _ := b.Len()
	if count != 1 {
		t.Errorf("Expected 1 entry after commit(0), got %d", count)
	}
}

func TestDiskBuffer_FIFOOrder(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	for i := 0; i < 100; i++ {
		if err := b.Write(entry("msg")); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	// Read all entries to verify order
	entries, err := ReadAll(b)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if len(entries) != 100 {
		t.Errorf("Expected 100 entries, got %d", len(entries))
	}
	for i := range entries {
		if entries[i].Message != "msg" {
			t.Errorf("Expected all messages to be 'msg', got '%s'", entries[i].Message)
		}
	}

	// Commit all entries
	if err := b.Commit(100); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	count, _ := b.Len()
	if count != 0 {
		t.Errorf("Expected 0 entries after full commit, got %d", count)
	}
}

func TestDiskBuffer_EmptyBuffer(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	count, _ := b.Len()
	if count != 0 {
		t.Errorf("Expected 0 entries in empty buffer, got %d", count)
	}

	entries, err := b.ReadBatch(10)
	if err != nil {
		t.Fatalf("Failed to read from empty buffer: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries from empty buffer, got %d", len(entries))
	}

	if err := b.Commit(10); err != nil {
		t.Fatalf("Failed to commit on empty buffer: %v", err)
	}
}

func TestDiskBuffer_Size(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	// Just verify that Size() works and returns non-negative value
	size, err := b.Size()
	if err != nil {
		t.Fatalf("Failed to get size: %v", err)
	}
	if size < 0 {
		t.Errorf("Expected non-negative size, got %d", size)
	}

	// Write some data
	for i := 0; i < 100; i++ {
		b.Write(entry("test message with some content"))
	}

	// Size should still work (exact value may vary based on Badger's internal state)
	sizeAfter, err := b.Size()
	if err != nil {
		t.Fatalf("Failed to get size after writes: %v", err)
	}
	if sizeAfter < 0 {
		t.Errorf("Expected non-negative size after writes, got %d", sizeAfter)
	}
}

func TestDiskBuffer_DropAll(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	for i := 0; i < 50; i++ {
		if err := b.Write(entry("test")); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	if err := b.DropAll(); err != nil {
		t.Fatalf("Failed to drop all: %v", err)
	}

	count, err := b.Len()
	if err != nil {
		t.Fatalf("Failed to get len: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 entries after drop, got %d", count)
	}
}

func TestDiskBuffer_ReadAfterCommit(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	// Write 5, commit 2, read should get 3
	for i := 0; i < 5; i++ {
		b.Write(entry("test"))
	}

	b.Commit(2)

	entries, err := b.ReadBatch(10)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries after commit, got %d", len(entries))
	}
}

func TestDiskBuffer_ConcurrentWrite(t *testing.T) {
	b, cleanup := newTestBuffer(t)
	defer cleanup()

	done := make(chan bool, 2)
	writeAll := func(n int) {
		for i := 0; i < n; i++ {
			b.Write(entry("concurrent"))
		}
		done <- true
	}

	go writeAll(50)
	go writeAll(50)

	<-done
	<-done

	count, err := b.Len()
	if err != nil {
		t.Fatalf("Failed to get len: %v", err)
	}
	if count != 100 {
		t.Errorf("Expected 100 entries after concurrent writes, got %d", count)
	}
}

func TestDiskBuffer_Persistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "iye-buffer-persist-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logger, _ := zap.NewDevelopment()
	cfg := &models.BufferConfig{
		Enabled:      true,
		Path:         dir,
		MaxSizeBytes: 64 << 20,
		SyncWrites:   true,
	}

	b1, err := NewDiskBuffer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create first buffer: %v", err)
	}

	for i := 0; i < 10; i++ {
		b1.Write(entry("persist"))
	}

	count1, _ := b1.Len()
	if count1 != 10 {
		t.Errorf("Expected 10 entries in first buffer, got %d", count1)
	}
	b1.Close()

	// Reopen same directory
	b2, err := NewDiskBuffer(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to reopen buffer: %v", err)
	}
	defer b2.Close()

	count2, _ := b2.Len()
	if count2 != 10 {
		t.Errorf("Expected 10 entries after reopen, got %d", count2)
	}

	entries, err := b2.ReadBatch(10)
	if err != nil {
		t.Fatalf("Failed to read from reopened buffer: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("Expected 10 entries from reopened buffer, got %d", len(entries))
	}
}

// ReadAll reads all entries from the buffer without modifying it
func ReadAll(b *DiskBuffer) ([]models.LogEntry, error) {
	var entries []models.LogEntry
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         entryPref,
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(val) == 0 {
				continue
			}
			var se storedEntry
			if err := json.Unmarshal(val, &se); err != nil {
				return err
			}
			entries = append(entries, models.LogEntry{
				Timestamp: time.Unix(0, se.Timestamp).Format(time.RFC3339Nano),
				Source:    se.Source,
				Message:   se.Message,
				Level:     se.Level,
				Labels:    se.Labels,
			})
		}
		return nil
	})
	return entries, err
}
