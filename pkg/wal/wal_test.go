package wal

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
)

func createTestConfig() *config.Config {
	return config.NewDefaultConfig("/tmp/gostorage_test")
}

func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	return dir
}

func TestWALWrite(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write some entries
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		seq, err := wal.Append(OpTypePut, []byte(key), []byte(values[i]))
		if err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}

		if seq != uint64(i+1) {
			t.Errorf("Expected sequence %d, got %d", i+1, seq)
		}
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify entries by replaying
	replayedEntries := make(map[string]string)

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut {
			replayedEntries[string(entry.Key)] = string(entry.Value)
		} else if entry.Type == OpTypeDelete {
			delete(replayedEntries, string(entry.Key))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify all entries are present
	for i, key := range keys {
		value, ok := replayedEntries[key]
		if !ok {
			t.Errorf("Entry for key %q not found", key)
			continue
		}

		if value != values[i] {
			t.Errorf("Expected value %q for key %q, got %q", values[i], key, value)
		}
	}
}

func TestWALDelete(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write and delete
	key := []byte("key1")
	value := []byte("value1")

	_, err = wal.Append(OpTypePut, key, value)
	if err != nil {
		t.Fatalf("Failed to append put entry: %v", err)
	}

	_, err = wal.Append(OpTypeDelete, key, nil)
	if err != nil {
		t.Fatalf("Failed to append delete entry: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify entries by replaying
	var deleted bool

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut && bytes.Equal(entry.Key, key) {
			if deleted {
				deleted = false // Key was re-added
			}
		} else if entry.Type == OpTypeDelete && bytes.Equal(entry.Key, key) {
			deleted = true
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if !deleted {
		t.Errorf("Expected key to be deleted")
	}
}

func TestWALLargeEntry(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create a large key and value (but not too large for a single record)
	key := make([]byte, 8*1024)    // 8KB
	value := make([]byte, 16*1024) // 16KB

	for i := range key {
		key[i] = byte(i % 256)
	}

	for i := range value {
		value[i] = byte((i * 2) % 256)
	}

	// Append the large entry
	_, err = wal.Append(OpTypePut, key, value)
	if err != nil {
		t.Fatalf("Failed to append large entry: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify by replaying
	var foundLargeEntry bool

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut && len(entry.Key) == len(key) && len(entry.Value) == len(value) {
			// Verify key
			for i := range key {
				if key[i] != entry.Key[i] {
					t.Errorf("Key mismatch at position %d: expected %d, got %d", i, key[i], entry.Key[i])
					return nil
				}
			}

			// Verify value
			for i := range value {
				if value[i] != entry.Value[i] {
					t.Errorf("Value mismatch at position %d: expected %d, got %d", i, value[i], entry.Value[i])
					return nil
				}
			}

			foundLargeEntry = true
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	if !foundLargeEntry {
		t.Error("Large entry not found in replay")
	}
}

func TestWALBatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create batch entries
	entries := []*Entry{
		{Type: OpTypePut, Key: []byte("batch1"), Value: []byte("value1")},
		{Type: OpTypePut, Key: []byte("batch2"), Value: []byte("value2")},
		{Type: OpTypePut, Key: []byte("batch3"), Value: []byte("value3")},
		{Type: OpTypeDelete, Key: []byte("batch2"), Value: nil}, // Delete batch2
	}

	// Write the batch using AppendBatch
	_, err = wal.AppendBatch(entries)
	if err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify by replaying
	replayedEntries := make(map[string]string)

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut {
			replayedEntries[string(entry.Key)] = string(entry.Value)
		} else if entry.Type == OpTypeDelete {
			delete(replayedEntries, string(entry.Key))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify entries
	expectedEntries := map[string]string{
		"batch1": "value1",
		"batch3": "value3",
		// batch2 should be deleted
	}

	for key, expectedValue := range expectedEntries {
		value, ok := replayedEntries[key]
		if !ok {
			t.Errorf("Entry for key %q not found", key)
			continue
		}

		if value != expectedValue {
			t.Errorf("Expected value %q for key %q, got %q", expectedValue, key, value)
		}
	}

	// Verify batch2 is deleted
	if _, ok := replayedEntries["batch2"]; ok {
		t.Errorf("Key batch2 should be deleted")
	}
}

func TestWALRecovery(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()

	// Write some entries in the first WAL
	wal1, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	_, err = wal1.Append(OpTypePut, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	if err := wal1.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Create a second WAL file
	wal2, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	_, err = wal2.Append(OpTypePut, []byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	if err := wal2.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify entries by replaying all WAL files in order
	entries := make(map[string]string)

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut {
			entries[string(entry.Key)] = string(entry.Value)
		} else if entry.Type == OpTypeDelete {
			delete(entries, string(entry.Key))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify all entries are present
	expected := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	for key, expectedValue := range expected {
		value, ok := entries[key]
		if !ok {
			t.Errorf("Entry for key %q not found", key)
			continue
		}

		if value != expectedValue {
			t.Errorf("Expected value %q for key %q, got %q", expectedValue, key, value)
		}
	}
}

func TestWALSyncModes(t *testing.T) {
	testCases := []struct {
		name     string
		syncMode config.SyncMode
	}{
		{"SyncNone", config.SyncNone},
		{"SyncBatch", config.SyncBatch},
		{"SyncImmediate", config.SyncImmediate},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := createTempDir(t)
			defer os.RemoveAll(dir)

			// Create config with specific sync mode
			cfg := createTestConfig()
			cfg.WALSyncMode = tc.syncMode

			wal, err := NewWAL(cfg, dir)
			if err != nil {
				t.Fatalf("Failed to create WAL: %v", err)
			}

			// Write some entries
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("key%d", i))
				value := []byte(fmt.Sprintf("value%d", i))

				_, err := wal.Append(OpTypePut, key, value)
				if err != nil {
					t.Fatalf("Failed to append entry: %v", err)
				}
			}

			// Close the WAL
			if err := wal.Close(); err != nil {
				t.Fatalf("Failed to close WAL: %v", err)
			}

			// Verify entries by replaying
			count := 0
			_, err = ReplayWALDir(dir, func(entry *Entry) error {
				if entry.Type == OpTypePut {
					count++
				}
				return nil
			})

			if err != nil {
				t.Fatalf("Failed to replay WAL: %v", err)
			}

			if count != 10 {
				t.Errorf("Expected 10 entries, got %d", count)
			}
		})
	}
}

func TestWALFragmentation(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create an entry that's guaranteed to be fragmented
	// Header size is 1 + 8 + 4 = 13 bytes, so allocate more than MaxRecordSize - 13 for the key
	keySize := MaxRecordSize - 10
	valueSize := MaxRecordSize * 2

	key := make([]byte, keySize)     // Just under MaxRecordSize to ensure key fragmentation
	value := make([]byte, valueSize) // Large value to ensure value fragmentation

	// Fill with recognizable patterns
	for i := range key {
		key[i] = byte(i % 256)
	}

	for i := range value {
		value[i] = byte((i * 3) % 256)
	}

	// Append the large entry - this should trigger fragmentation
	_, err = wal.Append(OpTypePut, key, value)
	if err != nil {
		t.Fatalf("Failed to append fragmented entry: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify by replaying
	var reconstructedKey []byte
	var reconstructedValue []byte
	var foundPut bool

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		if entry.Type == OpTypePut {
			foundPut = true
			reconstructedKey = entry.Key
			reconstructedValue = entry.Value
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Check that we found the entry
	if !foundPut {
		t.Fatal("Did not find PUT entry in replay")
	}

	// Verify key length matches
	if len(reconstructedKey) != keySize {
		t.Errorf("Key length mismatch: expected %d, got %d", keySize, len(reconstructedKey))
	}

	// Verify value length matches
	if len(reconstructedValue) != valueSize {
		t.Errorf("Value length mismatch: expected %d, got %d", valueSize, len(reconstructedValue))
	}

	// Check key content (first 10 bytes)
	for i := 0; i < 10 && i < len(key); i++ {
		if key[i] != reconstructedKey[i] {
			t.Errorf("Key mismatch at position %d: expected %d, got %d", i, key[i], reconstructedKey[i])
		}
	}

	// Check key content (last 10 bytes)
	for i := 0; i < 10 && i < len(key); i++ {
		idx := len(key) - 1 - i
		if key[idx] != reconstructedKey[idx] {
			t.Errorf("Key mismatch at position %d: expected %d, got %d", idx, key[idx], reconstructedKey[idx])
		}
	}

	// Check value content (first 10 bytes)
	for i := 0; i < 10 && i < len(value); i++ {
		if value[i] != reconstructedValue[i] {
			t.Errorf("Value mismatch at position %d: expected %d, got %d", i, value[i], reconstructedValue[i])
		}
	}

	// Check value content (last 10 bytes)
	for i := 0; i < 10 && i < len(value); i++ {
		idx := len(value) - 1 - i
		if value[idx] != reconstructedValue[idx] {
			t.Errorf("Value mismatch at position %d: expected %d, got %d", idx, value[idx], reconstructedValue[idx])
		}
	}

	// Verify random samples from the key and value
	for i := 0; i < 10; i++ {
		// Check random positions in the key
		keyPos := rand.Intn(keySize)
		if key[keyPos] != reconstructedKey[keyPos] {
			t.Errorf("Key mismatch at random position %d: expected %d, got %d", keyPos, key[keyPos], reconstructedKey[keyPos])
		}

		// Check random positions in the value
		valuePos := rand.Intn(valueSize)
		if value[valuePos] != reconstructedValue[valuePos] {
			t.Errorf("Value mismatch at random position %d: expected %d, got %d", valuePos, value[valuePos], reconstructedValue[valuePos])
		}
	}
}

func TestWALErrorHandling(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write some entries
	_, err = wal.Append(OpTypePut, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Try to write after close
	_, err = wal.Append(OpTypePut, []byte("key2"), []byte("value2"))
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got: %v", err)
	}

	// Try to sync after close
	err = wal.Sync()
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got: %v", err)
	}

	// Try to replay a non-existent file
	nonExistentPath := filepath.Join(dir, "nonexistent.wal")
	_, err = ReplayWALFile(nonExistentPath, func(entry *Entry) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error when replaying non-existent file")
	}
}

func TestAppendWithSequence(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write entries with specific sequence numbers
	testCases := []struct {
		key       string
		value     string
		seqNum    uint64
		entryType uint8
	}{
		{"key1", "value1", 100, OpTypePut},
		{"key2", "value2", 200, OpTypePut},
		{"key3", "value3", 300, OpTypePut},
		{"key4", "", 400, OpTypeDelete},
	}

	for _, tc := range testCases {
		seq, err := wal.AppendWithSequence(tc.entryType, []byte(tc.key), []byte(tc.value), tc.seqNum)
		if err != nil {
			t.Fatalf("Failed to append entry with sequence: %v", err)
		}

		if seq != tc.seqNum {
			t.Errorf("Expected sequence %d, got %d", tc.seqNum, seq)
		}
	}

	// Verify nextSequence was updated correctly (should be highest + 1)
	if wal.GetNextSequence() != 401 {
		t.Errorf("Expected next sequence to be 401, got %d", wal.GetNextSequence())
	}

	// Write a normal entry to verify sequence numbering continues correctly
	seq, err := wal.Append(OpTypePut, []byte("key5"), []byte("value5"))
	if err != nil {
		t.Fatalf("Failed to append normal entry: %v", err)
	}

	if seq != 401 {
		t.Errorf("Expected next normal entry to have sequence 401, got %d", seq)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Verify entries by replaying
	seqToKey := make(map[uint64]string)
	seqToValue := make(map[uint64]string)
	seqToType := make(map[uint64]uint8)

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		seqToKey[entry.SequenceNumber] = string(entry.Key)
		seqToValue[entry.SequenceNumber] = string(entry.Value)
		seqToType[entry.SequenceNumber] = entry.Type
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify all entries with specific sequence numbers
	for _, tc := range testCases {
		key, ok := seqToKey[tc.seqNum]
		if !ok {
			t.Errorf("Entry with sequence %d not found", tc.seqNum)
			continue
		}

		if key != tc.key {
			t.Errorf("Expected key %q for sequence %d, got %q", tc.key, tc.seqNum, key)
		}

		entryType, ok := seqToType[tc.seqNum]
		if !ok {
			t.Errorf("Type for sequence %d not found", tc.seqNum)
			continue
		}

		if entryType != tc.entryType {
			t.Errorf("Expected type %d for sequence %d, got %d", tc.entryType, tc.seqNum, entryType)
		}

		// Check value for non-delete operations
		if tc.entryType != OpTypeDelete {
			value, ok := seqToValue[tc.seqNum]
			if !ok {
				t.Errorf("Value for sequence %d not found", tc.seqNum)
				continue
			}

			if value != tc.value {
				t.Errorf("Expected value %q for sequence %d, got %q", tc.value, tc.seqNum, value)
			}
		}
	}

	// Also verify the normal append entry
	key, ok := seqToKey[401]
	if !ok {
		t.Error("Entry with sequence 401 not found")
	} else if key != "key5" {
		t.Errorf("Expected key 'key5' for sequence 401, got %q", key)
	}

	value, ok := seqToValue[401]
	if !ok {
		t.Error("Value for sequence 401 not found")
	} else if value != "value5" {
		t.Errorf("Expected value 'value5' for sequence 401, got %q", value)
	}
}

func TestAppendBatchWithSequence(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	cfg := createTestConfig()
	wal, err := NewWAL(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create a batch of entries with specific types
	startSeq := uint64(1000)
	entries := []*Entry{
		{
			Type:  OpTypePut,
			Key:   []byte("batch_key1"),
			Value: []byte("batch_value1"),
		},
		{
			Type:  OpTypeDelete,
			Key:   []byte("batch_key2"),
			Value: nil,
		},
		{
			Type:  OpTypePut,
			Key:   []byte("batch_key3"),
			Value: []byte("batch_value3"),
		},
		{
			Type:  OpTypeMerge,
			Key:   []byte("batch_key4"),
			Value: []byte("batch_value4"),
		},
	}

	// Write the batch with a specific starting sequence
	batchSeq, err := wal.AppendBatchWithSequence(entries, startSeq)
	if err != nil {
		t.Fatalf("Failed to append batch with sequence: %v", err)
	}

	if batchSeq != startSeq {
		t.Errorf("Expected batch sequence %d, got %d", startSeq, batchSeq)
	}

	// Verify nextSequence was updated correctly (incremented by 1, not batch size)
	expectedNextSeq := startSeq + 1
	if wal.GetNextSequence() != expectedNextSeq {
		t.Errorf("Expected next sequence to be %d, got %d", expectedNextSeq, wal.GetNextSequence())
	}

	// Write a normal entry and verify its sequence
	normalSeq, err := wal.Append(OpTypePut, []byte("normal_key"), []byte("normal_value"))
	if err != nil {
		t.Fatalf("Failed to append normal entry: %v", err)
	}

	if normalSeq != expectedNextSeq {
		t.Errorf("Expected normal entry sequence %d, got %d", expectedNextSeq, normalSeq)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Replay and verify all entries
	var replayedEntries []*Entry

	_, err = ReplayWALDir(dir, func(entry *Entry) error {
		replayedEntries = append(replayedEntries, entry)
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// We should have len(entries) + 1 entries (batch entries + normal entry)
	expectedCount := len(entries) + 1
	if len(replayedEntries) != expectedCount {
		t.Errorf("Expected %d total entries, got %d", expectedCount, len(replayedEntries))
	}

	// Verify batch entries (first len(entries) entries should match our batch)
	// All batch entries should have the same sequence number
	for i := 0; i < len(entries) && i < len(replayedEntries); i++ {
		replayed := replayedEntries[i]
		expected := entries[i]

		if replayed.SequenceNumber != startSeq {
			t.Errorf("Entry %d: expected sequence %d, got %d", i, startSeq, replayed.SequenceNumber)
		}
		if replayed.Type != expected.Type {
			t.Errorf("Entry %d: expected type %d, got %d", i, expected.Type, replayed.Type)
		}
		if string(replayed.Key) != string(expected.Key) {
			t.Errorf("Entry %d: expected key %q, got %q", i, string(expected.Key), string(replayed.Key))
		}
		if expected.Type != OpTypeDelete && string(replayed.Value) != string(expected.Value) {
			t.Errorf("Entry %d: expected value %q, got %q", i, string(expected.Value), string(replayed.Value))
		}
	}

	// Verify normal entry (should be the last entry)
	if len(replayedEntries) > len(entries) {
		normalEntry := replayedEntries[len(entries)]
		if normalEntry.SequenceNumber != normalSeq {
			t.Errorf("Expected normal entry sequence %d, got %d", normalSeq, normalEntry.SequenceNumber)
		}
		if string(normalEntry.Key) != "normal_key" {
			t.Errorf("Expected key 'normal_key', got %q", string(normalEntry.Key))
		}
		if string(normalEntry.Value) != "normal_value" {
			t.Errorf("Expected value 'normal_value', got %q", string(normalEntry.Value))
		}
	} else {
		t.Error("Normal entry not found")
	}
}
