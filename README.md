# csv-2-postgresql-go
A Go application that reads an arbitrary csv, determines it's schema, and loads it into a target PostgreSQL database. It supports SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, BOOL, and TEXT datatypes.

# Building and Testing

`make` - Download project dependencies, build, and run tests
`make test` - Run tests
`make test_watch` - Run tests in watch mode

# Usage

To use the application, first build with `make`, then execute the application providing as arguments the name of the csv file and the postgresql connection string to store data in.

`make`
`./bin/csv-2-postgresql test2.csv postgres://postgres:hello@localhost/test?sslmode=disable`
