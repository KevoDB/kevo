// ABOUTME: This file contains tests for SSTable header validation security
// ABOUTME: Tests ensure malicious header fields are rejected before processing
package sstable

import (
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
