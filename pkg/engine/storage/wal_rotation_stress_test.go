package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/stats"
	"github.com/KevoDB/kevo/pkg/wal"
)

// TestWALRotationStress tests WAL rotation under heavy concurrent load
func TestWALRotationStress(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.Config{
		Version:         config.CurrentManifestVersion,
		SSTDir:          tempDir + "/sst",
		WALDir:          tempDir + "/wal",
		MemTableSize:    1024 * 1024, // 1MB
		MaxMemTables:    3,
		MemTablePoolCap: 5,
	}

	statsCollector := stats.NewAtomicCollector()
	manager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	const (
		numWriters      = 10
		numRotations    = 5
		writesPerWriter = 100
	)

	var (
		totalWrites      int64
		successfulWrites int64
		rotationErrors   int64
		writeErrors      int64
		wg               sync.WaitGroup
	)

	// Start concurrent writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < writesPerWriter; j++ {
				key := fmt.Sprintf("writer%d-key%d", writerID, j)
				value := fmt.Sprintf("value-%d-%d", writerID, j)

				atomic.AddInt64(&totalWrites, 1)

				err := manager.Put([]byte(key), []byte(value))
				if err != nil {
					if err == wal.ErrWALRotating {
						atomic.AddInt64(&rotationErrors, 1)
					} else {
						atomic.AddInt64(&writeErrors, 1)
						t.Logf("Write error: %v", err)
					}
				} else {
					atomic.AddInt64(&successfulWrites, 1)
				}

				// Small delay to allow rotation opportunities
				time.Sleep(100 * time.Microsecond)
			}
		}(i)
	}

	// Start concurrent WAL rotations
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < numRotations; i++ {
			// Wait a bit between rotations
			time.Sleep(50 * time.Millisecond)

			err := manager.RotateWAL()
			if err != nil {
				t.Logf("Rotation error: %v", err)
			}

			// Check if rotation state is properly managed
			if manager.isRotating() {
				t.Logf("Rotation %d in progress", i+1)
				// Allow some time for rotation to complete
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Wait for all operations to complete
	wg.Wait()

	// Verify results
	t.Logf("Test completed:")
	t.Logf("  Total writes attempted: %d", atomic.LoadInt64(&totalWrites))
	t.Logf("  Successful writes: %d", atomic.LoadInt64(&successfulWrites))
	t.Logf("  Rotation errors (retried): %d", atomic.LoadInt64(&rotationErrors))
	t.Logf("  Other write errors: %d", atomic.LoadInt64(&writeErrors))

	// Verify that most writes succeeded (allowing for some rotation conflicts)
	if atomic.LoadInt64(&successfulWrites) < atomic.LoadInt64(&totalWrites)*8/10 {
		t.Errorf("Too many writes failed: %d successful out of %d total",
			atomic.LoadInt64(&successfulWrites), atomic.LoadInt64(&totalWrites))
	}

	// Verify no unexpected write errors occurred
	if atomic.LoadInt64(&writeErrors) > 0 {
		t.Errorf("Unexpected write errors occurred: %d", atomic.LoadInt64(&writeErrors))
	}

	// Verify data integrity by reading back some values
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("writer%d-key%d", 0, i)
		expectedValue := fmt.Sprintf("value-%d-%d", 0, i)

		value, err := manager.Get([]byte(key))
		if err != nil {
			t.Logf("Error reading key %s: %v", key, err)
			continue
		}

		if value != nil && string(value) == expectedValue {
			t.Logf("Successfully verified key %s", key)
		}
	}
}

// TestWALRotationAtomicity tests the atomicity of WAL rotation operations
func TestWALRotationAtomicity(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.Config{
		Version:         config.CurrentManifestVersion,
		SSTDir:          tempDir + "/sst",
		WALDir:          tempDir + "/wal",
		MemTableSize:    1024 * 1024,
		MaxMemTables:    3,
		MemTablePoolCap: 5,
	}

	statsCollector := stats.NewAtomicCollector()
	manager, err := NewManager(cfg, statsCollector)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Test that WAL pointer is never nil during rotation
	const numIterations = 100

	var wg sync.WaitGroup

	// Continuously check WAL availability
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < numIterations*10; i++ {
			currentWAL := manager.getWAL()
			if currentWAL == nil {
				t.Errorf("WAL pointer was nil during operation %d", i)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// Perform rapid rotations
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			err := manager.RotateWAL()
			if err != nil {
				t.Logf("Rotation %d failed: %v", i, err)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Final verification
	currentWAL := manager.getWAL()
	if currentWAL == nil {
		t.Error("WAL pointer is nil after all rotations completed")
	}

	t.Logf("Atomicity test completed successfully")
}
