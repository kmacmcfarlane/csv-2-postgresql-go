package main

// DatabaseWriter defines a generic interface to a database
type DatabaseWriter interface {

	// CreateDatabase creates a database if it does not exist
	CreateDatabase(name string) (err error)
	// CreateTable instantiates a table compatible with the given schema
	CreateTable(name string) error
	// Insert adds a record to the table
	Insert(values []string) error
}