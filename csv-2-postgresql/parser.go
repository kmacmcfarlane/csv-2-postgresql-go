package main

import (
	"github.com/kmacmcfarlane/csv-2-postgresql-go/schema"
	"go/types"
)

// Parse defines a generic schema parser
type Parser interface {

	// ParseSchema determines the names and types of each column
	ParseSchema() (result schema.Schema, err error)
	// Read returns the next record. When there are no more records, result == nil
	Read() (result []string, err error)
	// ParseColumnType returns the best matching type for the given record field value
	ParseColumnType(value string) types.BasicKind
}