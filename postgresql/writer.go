package postgresql

import "github.com/kmacmcfarlane/csv-2-postgresql-go/schema"

type Writer struct {
	schema schema.Schema
}

// CreateTable instantiates a table compatible with the given schema
func (w Writer) CreateTable(name string, schema schema.Schema) error {


}

// Insert adds a record to the table
func (w Writer) Insert(values []string) error {

}