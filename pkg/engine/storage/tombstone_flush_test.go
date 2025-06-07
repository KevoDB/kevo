// ABOUTME: Tests tombstone preservation during memtable flush to SSTable
// ABOUTME: Verifies deleted keys remain deleted after restart through SSTable persistence

package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/sstable"
	"github.com/KevoDB/kevo/pkg/stats"
)

// TestTombstonePreservationInFlush tests that tombstones (deletion markers)
// are properly written to SSTables during memtable flush and that deleted
// keys remain deleted after recovery.
func TestTombstonePreservationInFlush(t *testing.T) {
	// Create temporary directories for the test
	tempDir, err := os.MkdirTemp("", "tombstone-flush-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create subdirectories for SSTables and WAL
	sstDir := filepath.Join(tempDir, "sst")
	walDir := filepath.Join(tempDir, "wal")

	// Create configuration
	cfg := &config.Config{
		Version:         config.CurrentManifestVersion,
		SSTDir:          sstDir,
		WALDir:          walDir,
		MemTableSize:    1024, // Small size to force flush
		MemTablePoolCap: 2,
		MaxMemTables:    2,
	}

	// Create a stats collector
	statsCollector := stats.NewAtomicCollector()

	// Create a new storage manager
	manager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer manager.Close()

	// Step 1: Add a key with initial value
	testKey := []byte("test-key")
	initialValue := []byte("initial-value")
	err = manager.Put(testKey, initialValue)
	if err != nil {
		t.Fatalf("Failed to put initial value: %v", err)
	}

	// Verify the key is readable
	value, err := manager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if string(value) != string(initialValue) {
		t.Errorf("Expected initial value %s, got %s", initialValue, value)
	}

	// Step 2: Delete the key (creates tombstone in memtable)
	err = manager.Delete(testKey)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Verify the key is no longer accessible
	_, err = manager.Get(testKey)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after deletion, got: %v", err)
	}

	// Step 3: Force flush to write tombstone to SSTable
	err = manager.FlushMemTables()
	if err != nil {
		t.Fatalf("Failed to flush memtables: %v", err)
	}

	// Verify the key is still not accessible after flush
	_, err = manager.Get(testKey)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after flush, got: %v", err)
	}

	// Step 4: Verify tombstone was actually written to SSTable
	sstables := manager.GetSSTables()
	if len(sstables) == 0 {
		t.Fatal("Expected at least one SSTable to be created after flush")
	}

	// Check that the SSTable contains the tombstone
	foundTombstone := false
	for _, sstablePath := range sstables {
		reader, err := sstable.OpenReader(sstablePath)
		if err != nil {
			t.Fatalf("Failed to open SSTable %s: %v", sstablePath, err)
		}

		iter := reader.NewIterator()
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			if string(iter.Key()) == string(testKey) {
				value := iter.Value()
				tombstone := iter.IsTombstone()
				t.Logf("Found key %s in SSTable %s: value=%v (len=%d), tombstone=%v",
					testKey, sstablePath, value, len(value), tombstone)

				if tombstone {
					foundTombstone = true
					t.Logf("Found tombstone for key %s in SSTable %s", testKey, sstablePath)
				} else {
					t.Errorf("Found key %s in SSTable but it's not a tombstone (value: %v)", testKey, value)
				}
				break
			}
		}
		reader.Close()
	}

	if !foundTombstone {
		t.Error("Tombstone was not found in any SSTable - deletion not persisted!")
	}

	// Step 5: Close and reopen to simulate restart
	err = manager.Close()
	if err != nil {
		t.Fatalf("Failed to close manager: %v", err)
	}

	// Create a new manager to simulate recovery
	recoveredManager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("Failed to create recovered manager: %v", err)
	}
	defer recoveredManager.Close()

	// Step 6: Verify the key is still deleted after recovery
	_, err = recoveredManager.Get(testKey)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after recovery, got: %v", err)
	}

	t.Log("Tombstone preservation test passed - deletions survive restart!")
}

// TestTombstoneWithSubsequentPut tests that a tombstone followed by a new put
// works correctly after flush and recovery.
func TestTombstoneWithSubsequentPut(t *testing.T) {
	// Create temporary directories for the test
	tempDir, err := os.MkdirTemp("", "tombstone-put-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create subdirectories for SSTables and WAL
	sstDir := filepath.Join(tempDir, "sst")
	walDir := filepath.Join(tempDir, "wal")

	// Create configuration
	cfg := &config.Config{
		Version:         config.CurrentManifestVersion,
		SSTDir:          sstDir,
		WALDir:          walDir,
		MemTableSize:    1024, // Small size to force flush
		MemTablePoolCap: 2,
		MaxMemTables:    2,
	}

	// Create a stats collector
	statsCollector := stats.NewAtomicCollector()

	// Create a new storage manager
	manager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer manager.Close()

	testKey := []byte("test-key")

	// Step 1: Put initial value
	initialValue := []byte("initial-value")
	err = manager.Put(testKey, initialValue)
	if err != nil {
		t.Fatalf("Failed to put initial value: %v", err)
	}

	// Step 2: Delete the key
	err = manager.Delete(testKey)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Step 3: Put a new value for the same key
	newValue := []byte("new-value")
	err = manager.Put(testKey, newValue)
	if err != nil {
		t.Fatalf("Failed to put new value: %v", err)
	}

	// Verify the new value is accessible
	value, err := manager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get new value: %v", err)
	}
	if string(value) != string(newValue) {
		t.Errorf("Expected new value %s, got %s", newValue, value)
	}

	// Step 4: Force flush
	err = manager.FlushMemTables()
	if err != nil {
		t.Fatalf("Failed to flush memtables: %v", err)
	}

	// Verify the new value is still accessible after flush
	value, err = manager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get value after flush: %v", err)
	}
	if string(value) != string(newValue) {
		t.Errorf("Expected new value %s after flush, got %s", newValue, value)
	}

	// Step 5: Recovery test
	err = manager.Close()
	if err != nil {
		t.Fatalf("Failed to close manager: %v", err)
	}

	recoveredManager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("Failed to create recovered manager: %v", err)
	}
	defer recoveredManager.Close()

	// Verify the new value is still accessible after recovery
	recoveredValue, err := recoveredManager.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get value after recovery: %v", err)
	}
	if string(recoveredValue) != string(newValue) {
		t.Errorf("Expected new value %s after recovery, got %s", newValue, recoveredValue)
	}

	t.Log("Tombstone with subsequent put test passed!")
}
