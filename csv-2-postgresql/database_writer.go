package main

import "github.com/kmacmcfarlane/csv-2-postgresql-go/schema"

// DatabaseWriter defines a generic interface to a database
type DatabaseWriter interface {
	CreateTable(name string, schema schema.Schema)
	Insert(values []string)
}