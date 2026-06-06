package buffer

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/iye/iye/pkg/models"
	"go.uber.org/zap"
)

var (
	seqKey    = []byte("seq")
	entryPref = []byte("e/")
)

type DiskBuffer struct {
	db     *badger.DB
	config *models.BufferConfig
	logger *zap.Logger
	seq    *badger.Sequence
}

type storedEntry struct {
	ID        uint64            `json:"id"`
	Timestamp int64             `json:"ts"`
	Source    string            `json:"src"`
	Message   string            `json:"msg"`
	Level     string            `json:"lvl"`
	Labels    map[string]string `json:"lbl,omitempty"`
}

func NewDiskBuffer(config *models.BufferConfig, logger *zap.Logger) (*DiskBuffer, error) {
	opts := badger.DefaultOptions(config.Path)
	opts.SyncWrites = config.SyncWrites
	opts.Logger = nil
	opts.MetricsEnabled = false

	if config.MaxSizeBytes > 0 {
		opts.MemTableSize = config.MaxSizeBytes / 4
		if opts.MemTableSize < 64<<20 {
			opts.MemTableSize = 64 << 20
		}
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	seq, err := db.GetSequence(seqKey, 1000)
	if err != nil {
		db.Close()
		return nil, err
	}

	b := &DiskBuffer{
		db:     db,
		config: config,
		logger: logger.Named("buffer"),
		seq:    seq,
	}

	b.logger.Info("Disk buffer initialized",
		zap.String("path", config.Path),
		zap.Int64("max_size_bytes", config.MaxSizeBytes),
	)

	return b, nil
}

func (b *DiskBuffer) Write(entry models.LogEntry) error {
	nextSeq, err := b.seq.Next()
	if err != nil {
		return err
	}

	ts, _ := time.Parse(time.RFC3339Nano, entry.Timestamp)
	se := storedEntry{
		ID:        nextSeq,
		Timestamp: ts.UnixNano(),
		Source:    entry.Source,
		Message:   entry.Message,
		Level:     entry.Level,
		Labels:    entry.Labels,
	}

	data, err := json.Marshal(se)
	if err != nil {
		return err
	}

	key := makeKey(nextSeq)
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (b *DiskBuffer) ReadBatch(n int) ([]models.LogEntry, error) {
	if n <= 0 {
		return nil, nil
	}

	var entries []models.LogEntry
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         entryPref,
		}

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid() && len(entries) < n; it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
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

func (b *DiskBuffer) Commit(n int) error {
	if n <= 0 {
		return nil
	}

	return b.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         entryPref,
		})
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid() && count < n; it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
			count++
		}
		return nil
	})
}

func (b *DiskBuffer) Len() (int64, error) {
	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         entryPref,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

func (b *DiskBuffer) Size() (int64, error) {
	lsm, vlog := b.db.Size()
	return lsm + vlog, nil
}

func (b *DiskBuffer) Close() error {
	b.seq.Release()
	return b.db.Close()
}

func (b *DiskBuffer) DropAll() error {
	return b.db.DropAll()
}

func makeKey(seq uint64) []byte {
	buf := make([]byte, len(entryPref)+8)
	copy(buf, entryPref)
	binary.BigEndian.PutUint64(buf[len(entryPref):], seq)
	return buf
}
