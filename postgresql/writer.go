package postgresql

import "github.com/kmacmcfarlane/csv-2-postgresql-go/schema"

type Writer struct {
	schema schema.Schema
}

func (w Writer) Write(values []string) error {

}