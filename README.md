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

# Design

* main package (csv-2-postgresql) defines both the entry point to the application and the main interfaces (Parser and DatabaseWriter).
  * These interfaces have implementations using 3rd party packages for handling the CSV and Postgresql part of their operation.
  * This helps relieve the dependencies that must be brought into the main program, decreasing coupling.
* The schema package defines the domain model (which is simply the schema of the CSV file for this problem space).
* Both the CSV parser and Postgresql DB adapter use private helper methods to determine the types best suited for the data and convert to types compatible with the database.
* Unit tests cover all the business logic in the CSV parser and Postgresql database adapter.
* I neglected to use vendoring for this project to keep things simple, the latest versions of build and test dependencies are installed using the Makefile automatically when building or testing.
