package main

import "github.com/kmacmcfarlane/csv-2-postgresql-go/schema"

// DatabaseWriter defines a generic interface to a database
type DatabaseWriter interface {

	// CreateTable instantiates a table compatible with the given schema
	CreateTable(name string, schema schema.Schema) error
	// Insert adds a record to the table
	Insert(values []string) error
}