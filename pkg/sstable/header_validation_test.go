// ABOUTME: This file contains tests for SSTable header validation security
// ABOUTME: Tests ensure malicious header fields are rejected before processing
package sstable

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/KevoDB/kevo/pkg/sstable/footer"
)

// TestHeaderValidation_IndexOffsetBeyondFile tests rejection of malicious index offsets
func TestHeaderValidation_IndexOffsetBeyondFile(t *testing.T) {
	// Create a malicious footer with index offset beyond file size
	ft := &footer.Footer{
		Magic:       footer.FooterMagic,
		Version:     footer.CurrentVersion,
		IndexOffset: 10000, // Way beyond small file size
		IndexSize:   100,
		NumEntries:  1,
	}

	fileSize := int64(1000) // Small file size

	err := validateHeaderStructure(ft, fileSize)
	if err == nil {
		t.Error("Expected validation to fail with index offset beyond file size")
	}
}

// TestHeaderValidation_IndexSizeZero tests rejection of zero index size
func TestHeaderValidation_IndexSizeZero(t *testing.T) {
	ft := &footer.Footer{
		Magic:       footer.FooterMagic,
		Version:     footer.CurrentVersion,
		IndexOffset: 100,
		IndexSize:   0, // Invalid zero size
		NumEntries:  1,
	}

	fileSize := int64(1000)

	err := validateHeaderStructure(ft, fileSize)
	if err == nil {
		t.Error("Expected validation to fail with zero index size")
	}
}

// TestHeaderValidation_IndexOverlapsFooter tests rejection of index overlapping footer
func TestHeaderValidation_IndexOverlapsFooter(t *testing.T) {
	fileSize := int64(1000)
	footerStart := uint64(fileSize) - uint64(footer.FooterSize)

	ft := &footer.Footer{
		Magic:       footer.FooterMagic,
		Version:     footer.CurrentVersion,
		IndexOffset: footerStart - 10, // Start before footer
		IndexSize:   50,               // Extend into footer
		NumEntries:  1,
	}

	err := validateHeaderStructure(ft, fileSize)
	if err == nil {
		t.Error("Expected validation to fail with index overlapping footer")
	}
}

// TestHeaderValidation_BloomFilterBeyondFile tests rejection of malicious bloom filter offsets
func TestHeaderValidation_BloomFilterBeyondFile(t *testing.T) {
	ft := &footer.Footer{
		Magic:             footer.FooterMagic,
		Version:           footer.CurrentVersion,
		IndexOffset:       100,
		IndexSize:         200,
		NumEntries:        1,
		BloomFilterOffset: 10000, // Beyond file size
		BloomFilterSize:   100,
	}

	fileSize := int64(1000)

	err := validateHeaderStructure(ft, fileSize)
	if err == nil {
		t.Error("Expected validation to fail with bloom filter offset beyond file size")
	}
}

// TestHeaderValidation_BloomFilterInconsistent tests rejection of inconsistent bloom filter fields
func TestHeaderValidation_BloomFilterInconsistent(t *testing.T) {
	ft := &footer.Footer{
		Magic:             footer.FooterMagic,
		Version:           footer.CurrentVersion,
		IndexOffset:       100,
		IndexSize:         200,
		NumEntries:        1,
		BloomFilterOffset: 500, // Offset set
		BloomFilterSize:   0,   // But size is zero
	}

	fileSize := int64(1000)

	err := validateHeaderStructure(ft, fileSize)
	if err == nil {
		t.Error("Expected validation to fail with inconsistent bloom filter fields")
	}
}

// TestHeaderValidation_ZeroEntries tests rejection of SSTable with zero entries
func TestHeaderValidation_ZeroEntries(t *testing.T) {
	ft := &footer.Footer{
		Magic:       footer.FooterMagic,
		Version:     footer.CurrentVersion,
		IndexOffset: 100,
		IndexSize:   200,
		NumEntries:  0, // Invalid zero entries
	}

	fileSize := int64(1000)

	err := validateHeaderStructure(ft, fileSize)
	if err == nil {
		t.Error("Expected validation to fail with zero entries")
	}
}

// TestHeaderValidation_ValidHeader tests that valid headers pass validation
func TestHeaderValidation_ValidHeader(t *testing.T) {
	ft := &footer.Footer{
		Magic:       footer.FooterMagic,
		Version:     footer.CurrentVersion,
		IndexOffset: 100,
		IndexSize:   200,
		NumEntries:  10,
	}

	fileSize := int64(1000)

	err := validateHeaderStructure(ft, fileSize)
	if err != nil {
		t.Errorf("Expected valid header to pass validation, got error: %v", err)
	}
}

// TestHeaderValidation_IntegrationWithMaliciousFile tests opening a malicious file
func TestHeaderValidation_IntegrationWithMaliciousFile(t *testing.T) {
	// Create a temporary file with malicious footer
	tempFile, err := os.CreateTemp("", "malicious-sstable-*.sst")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write some data to make file size reasonable
	data := make([]byte, 1000)
	tempFile.Write(data)

	// Create malicious footer with index offset beyond file
	maliciousFooter := &footer.Footer{
		Magic:       footer.FooterMagic,
		Version:     footer.CurrentVersion,
		IndexOffset: 10000, // Way beyond file size
		IndexSize:   100,
		NumEntries:  1,
	}

	// Write malicious footer
	footerData := maliciousFooter.Encode()
	tempFile.Write(footerData)
	tempFile.Close()

	// Try to open the malicious file - should fail
	reader, err := OpenReader(tempFile.Name())
	if err == nil {
		reader.Close()
		t.Error("Expected OpenReader to fail with malicious file")
	}
}

// TestBloomFilterValidation_OversizedFilter tests rejection of oversized bloom filters
func TestBloomFilterValidation_OversizedFilter(t *testing.T) {
	// Test the validation function directly
	err := validateBloomFilterSize(100, 50, 80) // filterSize > remaining data
	if err == nil {
		t.Error("Expected validation to fail with oversized filter")
	}
}

// TestBloomFilterValidation_ZeroFilterSize tests rejection of zero-sized bloom filters
func TestBloomFilterValidation_ZeroFilterSize(t *testing.T) {
	err := validateBloomFilterSize(0, 10, 100) // zero size
	if err == nil {
		t.Error("Expected validation to fail with zero filter size")
	}
}

// TestBloomFilterValidation_ExtremelyLargeFilter tests rejection of extremely large filters
func TestBloomFilterValidation_ExtremelyLargeFilter(t *testing.T) {
	err := validateBloomFilterSize(128*1024*1024, 0, 128*1024*1024) // 128MB filter
	if err == nil {
		t.Error("Expected validation to fail with extremely large filter")
	}
}

// TestBloomFilterValidation_IntegerOverflow tests protection against integer overflow
func TestBloomFilterValidation_IntegerOverflow(t *testing.T) {
	// Test case where filterSize + pos would overflow
	err := validateBloomFilterSize(4294967295, 10, 100) // MaxUint32 + 10 would overflow
	if err == nil {
		t.Error("Expected validation to fail with potential integer overflow")
	}
}

// TestBloomFilterValidation_ValidFilter tests that valid filters pass validation
func TestBloomFilterValidation_ValidFilter(t *testing.T) {
	err := validateBloomFilterSize(1024, 0, 2048) // Valid 1KB filter
	if err != nil {
		t.Errorf("Expected valid filter to pass validation, got error: %v", err)
	}
}

// TestBloomFilterValidation_MaliciousSSTableWithBadBloomFilter tests malicious SSTable with bad bloom filter
func TestBloomFilterValidation_MaliciousSSTableWithBadBloomFilter(t *testing.T) {
	// Create a temporary file with malicious bloom filter
	tempFile, err := os.CreateTemp("", "malicious-bloom-*.sst")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write some data to make file size reasonable
	data := make([]byte, 1000)
	tempFile.Write(data)

	// Create malicious bloom filter data
	bloomData := make([]byte, 20)
	// Write block offset (8 bytes)
	binary.LittleEndian.PutUint64(bloomData[0:8], 0)
	// Write malicious filter size (4 bytes) - larger than remaining data
	binary.LittleEndian.PutUint32(bloomData[8:12], 100) // Claims 100 bytes but only 8 bytes remain

	// Write the bloom filter data
	tempFile.Write(bloomData)

	// Create footer with bloom filter
	ft := &footer.Footer{
		Magic:             footer.FooterMagic,
		Version:           footer.CurrentVersion,
		IndexOffset:       100,
		IndexSize:         200,
		NumEntries:        1,
		BloomFilterOffset: 1000,
		BloomFilterSize:   20,
	}

	// Write footer
	footerData := ft.Encode()
	tempFile.Write(footerData)
	tempFile.Close()

	// Try to open the malicious file - should fail
	reader, err := OpenReader(tempFile.Name())
	if err == nil {
		reader.Close()
		t.Error("Expected OpenReader to fail with malicious bloom filter")
	}
}
