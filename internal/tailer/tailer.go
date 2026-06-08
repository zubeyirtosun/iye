package tailer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/iye/iye/pkg/models"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	ErrTailerStopped = errors.New("tailer stopped")
	ErrFileNotFound  = errors.New("file not found")
	ErrMaxLineSize   = errors.New("line exceeds maximum size")
)

type FilePosition struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Inode  uint64 `json:"inode"`
}

type Tailer struct {
	config       *models.TailerConfig
	logger       *zap.Logger
	files        map[string]*trackedFile
	mu           sync.RWMutex
	output       chan *models.LogLine
	errors       chan error
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	running      atomic.Bool
	stats        TailerStats
	includeRegex *regexp.Regexp
	excludeRegex *regexp.Regexp
}

type trackedFile struct {
	path       string
	file       *os.File
	reader     *bufio.Reader
	position   FilePosition
	inode      uint64
	lastRead   time.Time
	rotated    bool
	mu         sync.Mutex
}

type TailerStats struct {
	LinesRead      atomic.Uint64
	BytesRead      atomic.Uint64
	FilesTracked   atomic.Uint64
	Rotations      atomic.Uint64
	Errors         atomic.Uint64
	TruncatedLines atomic.Uint64
}

func NewTailer(config *models.TailerConfig, logger *zap.Logger) (*Tailer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	t := &Tailer{
		config:  config,
		logger:  logger.Named("tailer"),
		files:   make(map[string]*trackedFile),
		output:  make(chan *models.LogLine, 10000),
		errors:  make(chan error, 100),
		ctx:     ctx,
		cancel:  cancel,
		running: *atomic.NewBool(false),
	}

	if config.IncludePattern != "" {
		re, err := regexp.Compile(config.IncludePattern)
		if err != nil {
			return nil, fmt.Errorf("invalid include pattern: %w", err)
		}
		t.includeRegex = re
	}

	if config.ExcludePattern != "" {
		re, err := regexp.Compile(config.ExcludePattern)
		if err != nil {
			return nil, fmt.Errorf("invalid exclude pattern: %w", err)
		}
		t.excludeRegex = re
	}

	return t, nil
}

func (t *Tailer) Start() error {
	if !t.running.CAS(false, true) {
		return errors.New("tailer already running")
	}

	t.logger.Info("Starting log tailer",
		zap.Strings("paths", t.config.Paths),
		zap.Duration("poll_interval", t.config.PollInterval),
	)

	initialPaths, err := t.expandPaths(t.config.Paths)
	if err != nil {
		t.running.Store(false)
		return fmt.Errorf("failed to expand paths: %w", err)
	}

	for _, path := range initialPaths {
		if err := t.addFile(path); err != nil {
			t.logger.Warn("Failed to add initial file", zap.String("path", path), zap.Error(err))
		}
	}

	t.wg.Add(2)
	go t.discoveryLoop()
	go t.tailLoop()

	return nil
}

func (t *Tailer) Stop() error {
	if !t.running.CAS(true, false) {
		return ErrTailerStopped
	}

	t.logger.Info("Stopping log tailer")
	t.cancel()
	t.wg.Wait()

	t.mu.Lock()
	for _, tf := range t.files {
		if tf.file != nil {
			tf.file.Close()
		}
	}
	t.mu.Unlock()

	close(t.output)
	close(t.errors)

	t.logger.Info("Log tailer stopped",
		zap.Uint64("lines_read", t.stats.LinesRead.Load()),
		zap.Uint64("bytes_read", t.stats.BytesRead.Load()),
	)

	return nil
}

func (t *Tailer) Output() <-chan *models.LogLine {
	return t.output
}

func (t *Tailer) Errors() <-chan error {
	return t.errors
}

func (t *Tailer) Stats() TailerStats {
	return t.stats
}

func (t *Tailer) expandPaths(patterns []string) ([]string, error) {
	var paths []string
	seen := make(map[string]bool)

	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("glob pattern %s: %w", pattern, err)
		}

		for _, match := range matches {
			info, err := os.Stat(match)
			if err != nil {
				continue
			}
			if info.IsDir() {
				continue
			}
			if !seen[match] {
				paths = append(paths, match)
				seen[match] = true
			}
		}
	}

	return paths, nil
}

func (t *Tailer) discoveryLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(t.config.PollInterval * 10)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.discoverFiles()
		}
	}
}

func (t *Tailer) discoverFiles() {
	paths, err := t.expandPaths(t.config.Paths)
	if err != nil {
		t.logger.Error("Failed to discover files", zap.Error(err))
		t.stats.Errors.Inc()
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	currentFiles := make(map[string]bool)
	for _, path := range paths {
		currentFiles[path] = true
		if _, exists := t.files[path]; !exists {
			if err := t.addFileLocked(path); err != nil {
				t.logger.Warn("Failed to add discovered file", zap.String("path", path), zap.Error(err))
			}
		}
	}

	for path, tf := range t.files {
		if !currentFiles[path] {
			t.logger.Debug("File no longer exists, closing", zap.String("path", path))
			if tf.file != nil {
				tf.file.Close()
			}
			delete(t.files, path)
		}
	}
}

func (t *Tailer) addFile(path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addFileLocked(path)
}

func (t *Tailer) addFileLocked(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	if info.IsDir() {
		return errors.New("path is a directory")
	}

	inode := getInode(info)
	var offset int64 = 0

	if !t.config.FromBeginning {
		offset = info.Size()
	}

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		file.Close()
		return fmt.Errorf("seek file: %w", err)
	}

	reader := bufio.NewReaderSize(file, t.config.ReadBufferSize)

	tf := &trackedFile{
		path:     path,
		file:     file,
		reader:   reader,
		position: FilePosition{Path: path, Offset: offset, Inode: inode},
		inode:    inode,
		lastRead: time.Now(),
	}

	t.files[path] = tf
	t.stats.FilesTracked.Inc()

	t.logger.Debug("Started tracking file",
		zap.String("path", path),
		zap.Int64("offset", offset),
		zap.Uint64("inode", inode),
	)

	return nil
}

func (t *Tailer) tailLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(t.config.PollInterval)
	defer ticker.Stop()

	buf := make([]byte, t.config.MaxLineSize)

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.processFiles(buf)
		}
	}
}

func (t *Tailer) processFiles(buf []byte) {
	t.mu.RLock()
	files := make([]*trackedFile, 0, len(t.files))
	for _, tf := range t.files {
		files = append(files, tf)
	}
	t.mu.RUnlock()

	for _, tf := range files {
		t.processFile(tf, buf)
	}
}

func (t *Tailer) processFile(tf *trackedFile, buf []byte) {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	if tf.file == nil {
		return
	}

	info, err := tf.file.Stat()
	if err != nil {
		t.logger.Error("Failed to stat file", zap.String("path", tf.path), zap.Error(err))
		t.stats.Errors.Inc()
		return
	}

	currentInode := getInode(info)
	if currentInode != tf.inode {
		t.logger.Info("Log rotation detected", zap.String("path", tf.path))
		t.handleRotation(tf, info)
		return
	}

	currentSize := info.Size()
	if currentSize < tf.position.Offset {
		t.logger.Info("File truncated", zap.String("path", tf.path))
		tf.position.Offset = 0
		if _, err := tf.file.Seek(0, io.SeekStart); err != nil {
			t.logger.Error("Failed to seek after truncation", zap.String("path", tf.path), zap.Error(err))
			t.stats.Errors.Inc()
			return
		}
		tf.reader.Reset(tf.file)
	}

	for {
		line, err := tf.reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == bufio.ErrBufferFull {
				t.logger.Warn("Line exceeds buffer size, truncating",
					zap.String("path", tf.path),
					zap.Int("max_size", t.config.MaxLineSize),
				)
				t.stats.TruncatedLines.Inc()
				line = line[:t.config.MaxLineSize-1]
				line = append(line, '\n')
				err = nil
			} else {
				t.logger.Error("Read error", zap.String("path", tf.path), zap.Error(err))
				t.stats.Errors.Inc()
				break
			}
		}

		origLen := len(line)
		if len(line) > 0 && line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}

		if len(line) == 0 {
			continue
		}

		if !t.shouldProcessLine(string(line)) {
			continue
		}

		tf.position.Offset += int64(origLen)
		tf.lastRead = time.Now()

		logLine := &models.LogLine{
			Timestamp: time.Now(),
			Source:    tf.path,
			Content:   string(line),
			Raw:       append([]byte(nil), line...),
			Labels:    map[string]string{"source": tf.path},
			Severity:  models.SeverityUnknown,
		}

		select {
		case t.output <- logLine:
			t.stats.LinesRead.Inc()
			t.stats.BytesRead.Add(uint64(len(line)))
		case <-t.ctx.Done():
			return
		default:
			t.logger.Warn("Output channel full, dropping line", zap.String("path", tf.path))
			t.stats.Errors.Inc()
		}
	}
}

func (t *Tailer) shouldProcessLine(line string) bool {
	if t.includeRegex != nil && !t.includeRegex.MatchString(line) {
		return false
	}
	if t.excludeRegex != nil && t.excludeRegex.MatchString(line) {
		return false
	}
	return true
}

func (t *Tailer) handleRotation(tf *trackedFile, info os.FileInfo) {
	t.stats.Rotations.Inc()

	oldFile := tf.file
	tf.file = nil

	newFile, err := os.Open(tf.path)
	if err != nil {
		t.logger.Error("Failed to open rotated file", zap.String("path", tf.path), zap.Error(err))
		t.stats.Errors.Inc()
		if oldFile != nil {
			oldFile.Close()
		}
		return
	}

	if _, err := newFile.Seek(0, io.SeekStart); err != nil {
		t.logger.Error("Failed to seek new file", zap.String("path", tf.path), zap.Error(err))
		newFile.Close()
		t.stats.Errors.Inc()
		return
	}

	tf.reader.Reset(newFile)
	tf.file = newFile
	tf.inode = getInode(info)
	tf.position.Offset = 0
	tf.position.Inode = tf.inode
	tf.rotated = true

	if oldFile != nil {
		oldFile.Close()
	}

	t.logger.Debug("Rotation handled", zap.String("path", tf.path), zap.Uint64("new_inode", tf.inode))
}

func (t *Tailer) GetTrackedFiles() []FilePosition {
	t.mu.RLock()
	defer t.mu.RUnlock()

	positions := make([]FilePosition, 0, len(t.files))
	for _, tf := range t.files {
		tf.mu.Lock()
		positions = append(positions, tf.position)
		tf.mu.Unlock()
	}
	return positions
}

func (t *Tailer) SetPosition(path string, offset int64) error {
	t.mu.RLock()
	tf, exists := t.files[path]
	t.mu.RUnlock()

	if !exists {
		return ErrFileNotFound
	}

	tf.mu.Lock()
	defer tf.mu.Unlock()

	if tf.file == nil {
		return errors.New("file not open")
	}

	if _, err := tf.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek: %w", err)
	}

	tf.reader.Reset(tf.file)
	tf.position.Offset = offset
	return nil
}