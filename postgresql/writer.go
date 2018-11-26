package postgresql

import (
	"errors"
	"fmt"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/schema"
	"go/types"
	"strings"
)

type Writer struct {
	db SQL
}

func NewWriter(db SQL) Writer {
	return Writer{
		db: db}
}

// CreateTable instantiates a table compatible with the given schema
func (w Writer) CreateTable(name string, schema schema.Schema) (err error) {

	// Validate
	if nil == schema.Columns || 0 == len(schema.Columns) {
		return errors.New("invalid schema: missing columns")
	}

	// Define the schema
	columnDefinitions := make([]string, len(schema.Columns))

	for i, col := range schema.Columns {

		columnType := w.getSQLColumnType(col.Kind)

		columnDefinitions[i] = fmt.Sprintf(`"%s" %s NOT NULL`, col.Name, columnType)
	}

	combinedColumns := strings.Join(columnDefinitions, ",\n")

	// Execute the query
	_, err = w.db.Exec(
		fmt.Sprintf(
`CREATE TABLE "%s" (
_id UUID PRIMARY KEY,
%s
);`, name, combinedColumns))

	if nil != err {
		return err
	}

	return err
}

// Insert adds a record to the table
func (w Writer) Insert(values []string, schema schema.Schema) (err error) {

	return err
}

func (w Writer) getSQLColumnType(goType types.BasicKind) (result string) {

	switch goType {
	case types.Int8:
		fallthrough
	case types.Int16:
		result = "SMALLINT"
	case types.Int32:
		result = "INTEGER"
	case types.Int64:
		result = "BIGINT"
	case types.Bool:
		result = "BOOL"
	case types.Float32:
		result = "FLOAT"
	case types.Float64:
		result = "DOUBLE"
	case types.String:
		result = "TEXT"
	}

	return result
}