package main

import (
	"github.com/kmacmcfarlane/csv-2-postgresql-go/schema"
	"go/types"
)

// Parse defines a generic schema parser
type Parser interface {
	ParseSchema() (result schema.Schema, err error)
	Read() (result []string, err error)
	ParseColumnType(value string) types.BasicKind
}