package csv

import (
	"encoding/csv"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/schema"
	"go/types"
	"io"
	"strconv"
)

type Parser struct {
	reader *csv.Reader
	firstRecord []string
}

func NewParser(reader io.Reader) Parser {
	return Parser{
		reader: csv.NewReader(reader)}
}

// ParseSchema reads the first two lines of the CSV file to determine header and column information
func (p Parser) ParseSchema() (result schema.Schema, err error) {

	// Get the header line
	headers, err := p.reader.Read()

	if nil != err {
		return result, err
	}

	result.Headers = headers

	// Get the next line to determine the type of each field
	p.firstRecord, err = p.reader.Read() // save this first record to avoid having to re-init the reader somehow

	result.Columns = make([]schema.Column, len(headers))
	for i, value := range p.firstRecord {

		result.Columns[i] = schema.Column{
			Name: headers[i],
			Kind: p.ParseColumnType(value)}
	}

	return result, err
}

//
func (p Parser) Read() (result []string, err error) {

	return result, err
}

// Determine the primitive type of a given string value. Fall back to string if nothing else matches.
func (p Parser) ParseColumnType(value string) types.BasicKind {

	// Signed ints
	// TODO: could scan the table and see if the column can be UInt, but might affect performance negatively...
	_, err := strconv.ParseInt(value, 10, 8)

	if err == nil {
		return types.Int8
	}

	_, err = strconv.ParseInt(value, 10, 16)

	if err == nil {
		return types.Int16
	}

	_, err = strconv.ParseInt(value, 10, 32)

	if err == nil {
		return types.Int32
	}

	_, err = strconv.ParseInt(value, 10, 64)

	if err == nil {
		return types.Int64
	}

	// Floats
	_, err = strconv.ParseFloat(value, 32)

	if err == nil {
		return types.Float32
	}

	_, err = strconv.ParseFloat(value, 64)

	if err == nil {
		return types.Float64
	}

	// Bool
	_, err = strconv.ParseBool(value)

	if err == nil {
		return types.Bool
	}

	// String is default
	return types.String
}