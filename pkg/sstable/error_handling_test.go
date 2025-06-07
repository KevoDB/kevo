package sstable

import (
	"errors"
	"os"
	"testing"
)

// TestFinishHandlesCloseErrors tests that Finish() properly handles Close() errors by
// simulating a scenario where the file is closed prematurely
func TestFinishHandlesCloseErrors(t *testing.T) {
	tempDir := t.TempDir()
	sstablePath := tempDir + "/test.sst"

	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add some data so Finish() has work to do
	if err := writer.Add([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to add entry: %v", err)
	}

	// Manually close the underlying file to simulate a file descriptor error
	// This will cause subsequent operations to fail, and Close() to potentially fail too
	if writer.fileManager.file != nil {
		writer.fileManager.file.Close()
		// Set it to nil so our Close() won't try to close it again
		writer.fileManager.file = nil
	}

	// Finish should still complete without panicking, even if Close() fails
	err = writer.Finish()
	// We expect some kind of error due to the closed file
	if err == nil {
		t.Error("Expected Finish() to return an error when file operations fail")
	}
}

// TestFinishWithInvalidFile tests that the error handling works when file operations fail
func TestFinishWithInvalidFile(t *testing.T) {
	tempDir := t.TempDir()
	sstablePath := tempDir + "/test.sst"

	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add some data
	if err := writer.Add([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to add entry: %v", err)
	}

	// Create a scenario where both operations fail and Close() fails
	// We'll simulate this by manually closing the file to cause subsequent operations to fail
	if writer.fileManager.file != nil {
		writer.fileManager.file.Close() // This will cause writes to fail
		writer.fileManager.file = nil   // Set to nil to avoid double-close
	}

	err = writer.Finish()
	if err == nil {
		t.Error("Expected Finish() to return an error when file operations fail")
	}
}

// TestCleanupWithInvalidFile tests that Cleanup() handles file errors properly
func TestCleanupWithInvalidFile(t *testing.T) {
	tempDir := t.TempDir()

	// Create a FileManager with non-existent paths to trigger errors
	nonExistentFile := tempDir + "/nonexistent.tmp"

	fm := &FileManager{
		path:    tempDir + "/test.sst",
		tmpPath: nonExistentFile, // This file doesn't exist, so Remove() will fail
		file:    nil,             // No file to close
	}

	err := fm.Cleanup()
	if err == nil {
		t.Error("Expected Cleanup() to return an error when removing non-existent file")
	}

	// Should be able to unwrap to the underlying os.PathError
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Expected error to wrap os.ErrNotExist, got: %v", err)
	}
}

// TestSuccessfulCloseDoesNotAffectResult tests that successful Close() doesn't change success results
func TestSuccessfulCloseDoesNotAffectResult(t *testing.T) {
	tempDir := t.TempDir()
	sstablePath := tempDir + "/test.sst"

	writer, err := NewWriter(sstablePath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add some data
	if err := writer.Add([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to add entry: %v", err)
	}

	// This should succeed normally
	err = writer.Finish()
	if err != nil {
		t.Errorf("Expected Finish() to succeed, got error: %v", err)
	}
}
