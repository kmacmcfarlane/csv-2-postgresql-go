package postgresql

import (
	"errors"
	"fmt"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/schema"
	"go/types"
	"strconv"
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
func (w Writer) CreateTable(tableName string, schema schema.Schema) (err error) {

	// Validate
	if nil == schema.Columns || 0 == len(schema.Columns) {
		return errors.New("invalid schema: missing columns")
	}

	// Remove existing table
	_, err = w.db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s";`, tableName))

	if nil != err {
		return err
	}

	// Define the schema
	columnDefinitions := make([]string, len(schema.Columns))

	for i, col := range schema.Columns {

		columnType := w.getSQLColumnType(col.Kind)

		columnDefinitions[i] = fmt.Sprintf(`"%s" %s NOT NULL`, col.Name, columnType)
	}

	combinedColumns := strings.Join(columnDefinitions, ",\n")

	// Execute the statement
	statementTemplate :=
`CREATE TABLE "%s" (
%s
);`

	statement := fmt.Sprintf(statementTemplate, tableName, combinedColumns)

	_, err = w.db.Exec(statement)

	if nil != err {
		return err
	}

	return err
}

// Insert adds a record to the table
func (w Writer) Insert(values []string, schema schema.Schema, tableName string) (err error) {

	var sb strings.Builder
	valuesLength := len(values)

	// Generate placeholder string. e.g. $1, $2, $3
	for i := 1; i < valuesLength + 1; i ++ {

		sb.WriteRune('$')
		sb.WriteString(strconv.Itoa(i))

		if i != valuesLength {
			sb.WriteRune(',')
			sb.WriteRune(' ')
		}
	}

	// Build the INSERT statement
	statement := fmt.Sprintf(`INSERT INTO "%s" VALUES (%s);`, tableName, sb.String())

	var valuesGeneric = make([]interface{}, len(values))
	for i, value := range values {
		valuesGeneric[i] = value
	}

	// Execute the statement
	_, err = w.db.Exec(statement, valuesGeneric...)

	if nil != err {
		return err
	}

	return err
}

// getSQLColumnType converts types.BasicKind values to corresponding postgresql column types
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
	default:
		panic(fmt.Sprintf("unsupported golang type: %d", goType))
	}

	return result
}