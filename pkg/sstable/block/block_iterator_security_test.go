// ABOUTME: This file contains security tests for block iterator delta encoding
// ABOUTME: Tests protect against malicious delta encoding that could cause buffer overruns
package block

import (
	"testing"
)

// TestDeltaEncodingValidation_ExcessiveSharedLength tests protection against excessive shared length
func TestDeltaEncodingValidation_ExcessiveSharedLength(t *testing.T) {
	currentKey := []byte("abc") // 3 bytes
	data := []byte("defgh")     // 5 bytes available

	// Try to claim 5 bytes shared from a 3-byte key
	err := validateDeltaEncoding(5, 2, currentKey, data)
	if err == nil {
		t.Error("Expected validation to fail with excessive shared length")
	}
}

// TestDeltaEncodingValidation_NilCurrentKey tests protection against nil current key
func TestDeltaEncodingValidation_NilCurrentKey(t *testing.T) {
	data := []byte("defgh")

	err := validateDeltaEncoding(1, 2, nil, data)
	if err == nil {
		t.Error("Expected validation to fail with nil current key")
	}
}

// TestDeltaEncodingValidation_InsufficientUnsharedData tests protection against insufficient unshared data
func TestDeltaEncodingValidation_InsufficientUnsharedData(t *testing.T) {
	currentKey := []byte("abc")
	data := []byte("de") // Only 2 bytes available

	// Try to claim 5 bytes unshared from 2-byte data
	err := validateDeltaEncoding(2, 5, currentKey, data)
	if err == nil {
		t.Error("Expected validation to fail with insufficient unshared data")
	}
}

// TestDeltaEncodingValidation_ExcessiveKeyLength tests protection against memory exhaustion
func TestDeltaEncodingValidation_ExcessiveKeyLength(t *testing.T) {
	currentKey := make([]byte, 40000) // 40KB key
	data := make([]byte, 40000)       // 40KB data

	// Try to create a 80KB key (40KB shared + 40KB unshared) - exceeds 64KB limit
	err := validateDeltaEncoding(40000, 40000, currentKey, data)
	if err == nil {
		t.Error("Expected validation to fail with excessive key length")
	}
}

// TestDeltaEncodingValidation_ValidDeltaEncoding tests that valid delta encoding passes
func TestDeltaEncodingValidation_ValidDeltaEncoding(t *testing.T) {
	currentKey := []byte("abcdef")
	data := []byte("ghijk")

	// Valid: 3 bytes shared + 2 bytes unshared = 5 bytes total
	err := validateDeltaEncoding(3, 2, currentKey, data)
	if err != nil {
		t.Errorf("Expected valid delta encoding to pass, got error: %v", err)
	}
}

// TestDeltaEncodingValidation_IntegerOverflow tests protection against integer overflow
func TestDeltaEncodingValidation_IntegerOverflow(t *testing.T) {
	currentKey := make([]byte, 1000)
	data := make([]byte, 1000)

	// Try to cause integer overflow with large values
	err := validateDeltaEncoding(65535, 65535, currentKey, data) // MaxUint16 + MaxUint16 would overflow
	if err == nil {
		t.Error("Expected validation to fail with potential integer overflow")
	}
}

// TestDeltaEncodingValidation_ZeroSharedLength tests edge case with zero shared length
func TestDeltaEncodingValidation_ZeroSharedLength(t *testing.T) {
	currentKey := []byte("abc")
	data := []byte("defgh")

	// Valid: 0 bytes shared + 3 bytes unshared = 3 bytes total
	err := validateDeltaEncoding(0, 3, currentKey, data)
	if err != nil {
		t.Errorf("Expected zero shared length to be valid, got error: %v", err)
	}
}

// TestDeltaEncodingValidation_MaxValidKeyLength tests boundary condition at max key length
func TestDeltaEncodingValidation_MaxValidKeyLength(t *testing.T) {
	currentKey := make([]byte, 32000) // 32KB key
	data := make([]byte, 32000)       // 32KB data

	// Valid: 32KB shared + 32KB unshared = 64KB total (exactly at limit)
	err := validateDeltaEncoding(32000, 32000, currentKey, data)
	if err != nil {
		t.Errorf("Expected max valid key length to pass, got error: %v", err)
	}
}
