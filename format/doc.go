// Package format implements low-level parsing of SQL CE binary file format.
//
// This package handles the physical layout: file headers, page I/O,
// page type classification, catalog metadata extraction, type mapping,
// row record parsing, and encryption detection.
//
// Most users should use the higher-level [engine] package instead.
package format
