// ABOUTME: Fuzz tests for memtable package to find edge cases and race conditions
// ABOUTME: Tests key-value operations, iterator behavior, and concurrent access patterns
package memtable

import (
	"bytes"
	"sync"
	"testing"
)

// FuzzPutGetDelete tests Put, Get, and Delete operations with arbitrary keys and values
func FuzzPutGetDelete(f *testing.F) {
	// Seed with some basic test cases
	f.Add([]byte("key"), []byte("value"), uint64(1))
	f.Add([]byte(""), []byte(""), uint64(0))
	f.Add([]byte("key"), []byte(""), uint64(1))
	f.Add([]byte(""), []byte("value"), uint64(1))
	f.Add([]byte("very_long_key_that_might_cause_issues"), []byte("very_long_value_that_might_cause_issues"), uint64(1000))

	f.Fuzz(func(t *testing.T, key, value []byte, seqNum uint64) {
		mt := NewMemTable()

		// Test Put operation
		mt.Put(key, value, seqNum)

		// Test Get operation
		retrievedValue, found := mt.Get(key)
		if !found {
			t.Errorf("Put key should be found after insertion")
			return
		}
		if !bytes.Equal(retrievedValue, value) {
			t.Errorf("Retrieved value doesn't match inserted value")
			return
		}

		// Test Contains operation
		if !mt.Contains(key) {
			t.Errorf("Contains should return true for inserted key")
			return
		}

		// Test Delete operation
		mt.Delete(key, seqNum+1)

		// After deletion, Get should return nil value but found=true (tombstone)
		retrievedValue, found = mt.Get(key)
		if !found {
			t.Errorf("Deleted key should still be found (tombstone)")
			return
		}
		if retrievedValue != nil {
			t.Errorf("Deleted key should return nil value, got %v", retrievedValue)
			return
		}

		// Contains should still return true for deleted key
		if !mt.Contains(key) {
			t.Errorf("Contains should return true for deleted key (tombstone)")
			return
		}

		// Test multiple puts with same key but different sequence numbers
		mt.Put(key, value, seqNum+10)
		retrievedValue, found = mt.Get(key)
		if !found {
			t.Errorf("Re-inserted key should be found")
			return
		}
		if !bytes.Equal(retrievedValue, value) {
			t.Errorf("Re-inserted value doesn't match")
			return
		}
	})
}

// FuzzIteratorSeek tests iterator Seek operations with arbitrary keys
func FuzzIteratorSeek(f *testing.F) {
	// Seed with some basic test cases
	f.Add([]byte("seek_key"))
	f.Add([]byte(""))
	f.Add([]byte("a"))
	f.Add([]byte("zzz"))

	f.Fuzz(func(t *testing.T, seekKey []byte) {
		mt := NewMemTable()

		// Insert some test data
		testKeys := [][]byte{
			[]byte("apple"),
			[]byte("banana"),
			[]byte("cherry"),
			[]byte("date"),
			[]byte("elderberry"),
		}
		for i, key := range testKeys {
			mt.Put(key, []byte("value"), uint64(i+1))
		}

		// Test iterator seek
		iter := mt.NewIterator()
		iter.Seek(seekKey)

		// If iterator is valid, the key should be >= seekKey
		if iter.Valid() {
			currentKey := iter.Key()
			if bytes.Compare(currentKey, seekKey) < 0 {
				t.Errorf("Iterator positioned at key %s which is < seek key %s", string(currentKey), string(seekKey))
				return
			}
		}

		// Test iteration from seek position
		validCount := 0
		for iter.Valid() && validCount < 100 { // Limit iterations to prevent infinite loops
			key := iter.Key()
			value := iter.Value()
			
			// Basic sanity checks
			if key == nil {
				t.Errorf("Iterator returned nil key")
				return
			}
			if iter.ValueType() == TypeValue && value == nil {
				t.Errorf("Iterator returned nil value for TypeValue entry")
				return
			}
			if iter.ValueType() == TypeDeletion && value != nil {
				t.Errorf("Iterator returned non-nil value for TypeDeletion entry")
				return
			}

			iter.Next()
			validCount++
		}
	})
}

// FuzzSequenceNumbers tests sequence number edge cases
func FuzzSequenceNumbers(f *testing.F) {
	// Seed with edge cases
	f.Add([]byte("key"), uint64(0))
	f.Add([]byte("key"), uint64(1))
	f.Add([]byte("key"), uint64(18446744073709551615)) // max uint64

	f.Fuzz(func(t *testing.T, key []byte, seqNum uint64) {
		mt := NewMemTable()

		// Test sequence number handling
		mt.Put(key, []byte("value1"), seqNum)
		
		// Insert with different sequence numbers
		if seqNum > 0 {
			mt.Put(key, []byte("value2"), seqNum-1) // Older
		}
		if seqNum < 18446744073709551615 {
			mt.Put(key, []byte("value3"), seqNum+1) // Newer
		}

		// The most recent value should be returned
		value, found := mt.Get(key)
		if !found {
			t.Errorf("Key should be found")
			return
		}
		
		// Determine what the expected value should be based on sequence numbers
		expectedValue := []byte("value1")
		if seqNum < 18446744073709551615 {
			expectedValue = []byte("value3") // This would be the newest
		}
		
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Expected value %s, got %s", string(expectedValue), string(value))
			return
		}

		// Verify sequence number ordering in skip list
		iter := mt.NewIterator()
		iter.Seek(key)
		
		if iter.Valid() && bytes.Equal(iter.Key(), key) {
			// The first occurrence should have the highest sequence number
			firstSeqNum := iter.SequenceNumber()
			
			// Check all entries with the same key
			for iter.Valid() && bytes.Equal(iter.Key(), key) {
				currentSeqNum := iter.SequenceNumber()
				if currentSeqNum > firstSeqNum {
					t.Errorf("Sequence numbers not properly ordered: found %d after %d", currentSeqNum, firstSeqNum)
					return
				}
				iter.Next()
			}
		}

		// Test GetNextSequenceNumber
		nextSeq := mt.GetNextSequenceNumber()
		// Handle max uint64 edge case - can't increment beyond max
		if seqNum < 18446744073709551615 {
			if nextSeq <= seqNum {
				t.Errorf("GetNextSequenceNumber should return value > %d, got %d", seqNum, nextSeq)
				return
			}
		} else {
			// For max uint64, nextSeq should be seqNum+1 which wraps or stays at max
			if nextSeq < seqNum && nextSeq != 0 {
				t.Errorf("GetNextSequenceNumber handling of max uint64 unexpected: got %d", nextSeq)
				return
			}
		}
	})
}

// FuzzConcurrentOperations tests concurrent access patterns
func FuzzConcurrentOperations(f *testing.F) {
	// Seed with some test data
	f.Add([]byte("key1"), []byte("value1"))
	f.Add([]byte("key2"), []byte("value2"))
	f.Add([]byte(""), []byte(""))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		mt := NewMemTable()
		
		// Number of goroutines
		const numGoroutines = 10
		const operationsPerGoroutine = 5
		
		var wg sync.WaitGroup
		
		// Start concurrent operations
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				
				for j := 0; j < operationsPerGoroutine; j++ {
					seqNum := uint64(goroutineID*operationsPerGoroutine + j + 1)
					
					// Mix of operations
					switch j % 4 {
					case 0:
						mt.Put(key, value, seqNum)
					case 1:
						mt.Get(key)
					case 2:
						mt.Contains(key)
					case 3:
						mt.Delete(key, seqNum)
					}
				}
			}(i)
		}
		
		// Wait for all operations to complete
		wg.Wait()
		
		// Verify memtable is still in a consistent state
		// Test basic operations still work
		mt.Put([]byte("test_key"), []byte("test_value"), 1000)
		value, found := mt.Get([]byte("test_key"))
		if !found || !bytes.Equal(value, []byte("test_value")) {
			t.Errorf("MemTable inconsistent after concurrent operations")
			return
		}
		
		// Test iterator still works
		iter := mt.NewIterator()
		iter.SeekToFirst()
		
		validCount := 0
		for iter.Valid() && validCount < 1000 { // Limit to prevent infinite loops
			if iter.Key() == nil {
				t.Errorf("Iterator returned nil key after concurrent operations")
				return
			}
			iter.Next()
			validCount++
		}
		
		// Test immutable transition
		mt.SetImmutable()
		if !mt.IsImmutable() {
			t.Errorf("MemTable should be immutable after SetImmutable")
			return
		}
		
		// Operations on immutable memtable should not crash
		mt.Put([]byte("should_not_insert"), []byte("value"), 2000)
		mt.Delete([]byte("should_not_delete"), 2001)
		
		// Get and Contains should still work on immutable memtable
		mt.Get([]byte("test_key"))
		mt.Contains([]byte("test_key"))
	})
}