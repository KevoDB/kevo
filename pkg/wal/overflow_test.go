package wal

import (
	"math"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
)

// TestSequenceNumberOverflow tests that sequence number overflow is properly detected
func TestSequenceNumberOverflow(t *testing.T) {
	tempDir := t.TempDir()
	
	cfg := &config.Config{
		WALDir:       tempDir,
		WALSyncMode:  config.SyncNone,
		WALSyncBytes: 0,
		WALMaxSize:   1024 * 1024,
	}

	wal, err := NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Set sequence number to near overflow
	wal.nextSequence = MaxSequenceNumber

	// This should trigger overflow error
	_, err = wal.Append(OpTypePut, []byte("test"), []byte("value"))
	if err != ErrSequenceOverflow {
		t.Errorf("Expected ErrSequenceOverflow, got: %v", err)
	}
}

// TestSequenceNumberOverflowBatch tests batch overflow detection
func TestSequenceNumberOverflowBatch(t *testing.T) {
	tempDir := t.TempDir()
	
	cfg := &config.Config{
		WALDir:       tempDir,
		WALSyncMode:  config.SyncNone,
		WALSyncBytes: 0,
		WALMaxSize:   1024 * 1024,
	}

	wal, err := NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Set sequence number to near overflow, but allow for a small batch
	wal.nextSequence = MaxSequenceNumber - 5

	// Create a batch that would overflow
	entries := []*Entry{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
		{Key: []byte("key6"), Value: []byte("value6")}, // This should cause overflow
	}

	_, err = wal.AppendBatch(entries)
	if err != ErrSequenceOverflow {
		t.Errorf("Expected ErrSequenceOverflow for batch, got: %v", err)
	}
}

// TestSequenceNumberOverflowWithSequence tests AppendWithSequence overflow detection
func TestSequenceNumberOverflowWithSequence(t *testing.T) {
	tempDir := t.TempDir()
	
	cfg := &config.Config{
		WALDir:       tempDir,
		WALSyncMode:  config.SyncNone,
		WALSyncBytes: 0,
		WALMaxSize:   1024 * 1024,
	}

	wal, err := NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Try to append with sequence number at overflow threshold
	_, err = wal.AppendWithSequence(OpTypePut, []byte("test"), []byte("value"), MaxSequenceNumber)
	if err != ErrSequenceOverflow {
		t.Errorf("Expected ErrSequenceOverflow for AppendWithSequence, got: %v", err)
	}
}

// TestSequenceNumberWarningThreshold tests that warnings are logged appropriately
func TestSequenceNumberWarningThreshold(t *testing.T) {
	tempDir := t.TempDir()
	
	cfg := &config.Config{
		WALDir:       tempDir,
		WALSyncMode:  config.SyncNone,
		WALSyncBytes: 0,
		WALMaxSize:   1024 * 1024,
	}

	wal, err := NewWAL(cfg, tempDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Set sequence number to warning threshold
	wal.nextSequence = SequenceWarningThreshold

	// This should log a warning but succeed
	_, err = wal.Append(OpTypePut, []byte("test"), []byte("value"))
	if err != nil {
		t.Errorf("Expected no error at warning threshold, got: %v", err)
	}

	// Verify warning flag is set
	if !wal.overflowWarning {
		t.Error("Expected overflow warning flag to be set")
	}

	// Second call should not log again (no additional verification since we can't easily capture logs)
	_, err = wal.Append(OpTypePut, []byte("test2"), []byte("value2"))
	if err != nil {
		t.Errorf("Expected no error on second append, got: %v", err)
	}
}

// TestSequenceNumberConstants verifies our constants are reasonable
func TestSequenceNumberConstants(t *testing.T) {
	// Verify MaxSequenceNumber is less than math.MaxUint64
	if MaxSequenceNumber >= math.MaxUint64 {
		t.Errorf("MaxSequenceNumber should be less than math.MaxUint64")
	}

	// Verify there's a reasonable safety margin
	if math.MaxUint64-MaxSequenceNumber != 1_000_000 {
		t.Errorf("Expected 1 million sequence safety margin, got: %d", math.MaxUint64-MaxSequenceNumber)
	}

	// Verify warning threshold is before max
	if SequenceWarningThreshold >= MaxSequenceNumber {
		t.Errorf("SequenceWarningThreshold should be less than MaxSequenceNumber")
	}

	// Verify warning margin
	if MaxSequenceNumber-SequenceWarningThreshold != 9_000_000 {
		t.Errorf("Expected 9 million sequence warning margin, got: %d", MaxSequenceNumber-SequenceWarningThreshold)
	}
}