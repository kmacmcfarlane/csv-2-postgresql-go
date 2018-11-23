package csv

import (
	"encoding/csv"
	"errors"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/schema"
	"go/types"
	"io"
	"strconv"
)

type Parser struct {
	reader *csv.Reader
	firstRecord []string
	schema *schema.Schema
}

func NewParser(reader io.Reader) *Parser {

	csvReader := csv.NewReader(reader)

	return &Parser{
		reader: csvReader}
}

// ParseSchema reads the first two lines of the CSV file to determine header and column information
func (p *Parser) ParseSchema() (result schema.Schema, err error) {

	if nil != p.schema {
		return *p.schema, err
	}

	// Get the header line
	headers, err := p.reader.Read()

	if nil != err {
		return result, err
	}

	result.Headers = headers

	// Get the next line to determine the type of each field
	p.firstRecord, err = p.reader.Read() // save this first record to avoid having to re-init the reader somehow

	if nil != err {
		return result, err
	}

	result.Columns = make([]schema.Column, len(headers))
	for i, value := range p.firstRecord {

		columnType := p.ParseColumnType(value)

		result.Columns[i] = schema.Column{
			Name: headers[i],
			Kind: columnType}
	}

	p.schema = &result

	return result, err
}

// Read returns records one at a time. It will return io.EOF as err when there are no more records
func (p *Parser) Read() (result []string, err error) {

	if nil == p.schema {
		return result, errors.New("read called before parsing schema")
	}

	// Return the first record used during schema creation
	if p.firstRecord != nil {

		result := p.firstRecord

		p.firstRecord = nil

		return result, err
	}

	return p.reader.Read()
}

// Determine the primitive type of a given string value. Fall back to string if nothing else matches.
func (p *Parser) ParseColumnType(value string) types.BasicKind {

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