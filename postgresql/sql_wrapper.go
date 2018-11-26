package postgresql

import "database/sql"

type SQL interface {
	Exec(query string, args ...interface{}) (Result, error)
	Close() error
}

type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

type SQLWrapper struct {
	inner *sql.DB
}

func NewSQLWrapper(inner *sql.DB) SQLWrapper {
	return SQLWrapper{
		inner: inner}
}

func (s SQLWrapper) Exec(query string, args ...interface{}) (Result, error) {
	return s.inner.Exec(query, args...)
}

func (s SQLWrapper) Close() error {
	return s.inner.Close()
}