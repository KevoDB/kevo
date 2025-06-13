package wal

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestBatchOperations(t *testing.T) {
	batch := NewBatch()

	// Test initially empty
	if batch.Count() != 0 {
		t.Errorf("Expected empty batch, got count %d", batch.Count())
	}

	// Add operations
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Delete([]byte("key3"))

	// Check count
	if batch.Count() != 3 {
		t.Errorf("Expected batch with 3 operations, got %d", batch.Count())
	}

	// Check size calculation
	expectedSize := BatchHeaderSize                         // count + seq
	expectedSize += 1 + 4 + 4 + len("key1") + len("value1") // type + keylen + vallen + key + value
	expectedSize += 1 + 4 + 4 + len("key2") + len("value2") // type + keylen + vallen + key + value
	expectedSize += 1 + 4 + len("key3")                     // type + keylen + key (no value for delete)

	if batch.Size() != expectedSize {
		t.Errorf("Expected batch size %d, got %d", expectedSize, batch.Size())
	}

	// Test reset
	batch.Reset()
	if batch.Count() != 0 {
		t.Errorf("Expected empty batch after reset, got count %d", batch.Count())
	}
}

func TestBatchEncoding(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create batch entries
	entries := []*Entry{
		{Type: OpTypePut, Key: []byte("key1"), Value: []byte("value1")},
		{Type: OpTypePut, Key: []byte("key2"), Value: []byte("value2")},
		{Type: OpTypeDelete, Key: []byte("key3"), Value: nil},
	}

	// Write the batch using AppendBatch
	startSeq, err := wal.AppendBatch(entries)
	if err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Check sequence
	if startSeq == 0 {
		t.Errorf("Batch sequence number not set")
	}

	// Close WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Replay and verify individual entries
	var replayedEntries []*Entry

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		replayedEntries = append(replayedEntries, entry)
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if len(replayedEntries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(replayedEntries))
	}

	// Verify individual entries match the batch operations
	expectedKeys := []string{"key1", "key2", "key3"}
	expectedValues := [][]byte{[]byte("value1"), []byte("value2"), nil} // key3 is deleted
	expectedTypes := []uint8{OpTypePut, OpTypePut, OpTypeDelete}

	for i, entry := range replayedEntries {
		if string(entry.Key) != expectedKeys[i] {
			t.Errorf("Entry %d: expected key %s, got %s", i, expectedKeys[i], string(entry.Key))
		}
		if entry.Type != expectedTypes[i] {
			t.Errorf("Entry %d: expected type %d, got %d", i, expectedTypes[i], entry.Type)
		}
		if expectedValues[i] == nil && entry.Value != nil {
			t.Errorf("Entry %d: expected nil value, got %v", i, entry.Value)
		} else if expectedValues[i] != nil && string(entry.Value) != string(expectedValues[i]) {
			t.Errorf("Entry %d: expected value %s, got %s", i, string(expectedValues[i]), string(entry.Value))
		}
	}
}

func TestEmptyBatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create empty batch
	batch := NewBatch()

	// Try to write empty batch
	err = batch.Write(wal)
	if err != ErrEmptyBatch {
		t.Errorf("Expected ErrEmptyBatch, got: %v", err)
	}

	// Close WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}
}

func TestLargeBatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create a batch that will exceed the maximum record size
	batch := NewBatch()

	// Add many large key-value pairs
	largeValue := make([]byte, 4096) // 4KB
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		batch.Put(key, largeValue)
	}

	// Verify the batch is too large
	if batch.Size() <= MaxRecordSize {
		t.Fatalf("Expected batch size > %d, got %d", MaxRecordSize, batch.Size())
	}

	// Try to write the large batch
	err = batch.Write(wal)
	if err == nil {
		t.Error("Expected error when writing large batch")
	}

	// Check that the error is ErrBatchTooLarge
	if err != nil && !bytes.Contains([]byte(err.Error()), []byte("batch too large")) {
		t.Errorf("Expected ErrBatchTooLarge, got: %v", err)
	}

	// Close WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}
}
