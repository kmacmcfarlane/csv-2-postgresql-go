package main

import (
	"database/sql"
	"fmt"
	"github.com/iancoleman/strcase"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/csv"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/postgresql"
	_ "github.com/lib/pq"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {

	// Parse arguments
	args := os.Args[1:]

	if len(args) != 2 {
		usage()
		os.Exit(1)
	}

	inputFile := args[0]
	connectionString := args[1]

	db, err := sql.Open("postgres", connectionString)

	if nil != err {
		panic(err)
	}

	defer db.Close()

	sqlWrapper := postgresql.NewSQLWrapper(db)

	var dbWriter DatabaseWriter = postgresql.NewWriter(sqlWrapper)

	// Parse schema of input
	file, err := os.Open(inputFile)

	if nil != err {
		panic(fmt.Sprintf("error opening file for reading: %s", err.Error()))
	}

	var parser Parser = csv.NewParser(file)

	schema, err := parser.ParseSchema()

	if nil != err {
		panic(fmt.Sprintf("error parsing csv file: %s", err.Error()))
	}

	// Create table
	_, filenameAlone := filepath.Split(file.Name())
	filenameAlone = strings.Replace(filenameAlone, ".csv", "", 1)
	tableName := strcase.ToSnake(filenameAlone)

	err = dbWriter.CreateTable(tableName, schema)

	if nil != err {
		panic(fmt.Sprintf("error creating db table: %s", err.Error()))
	}

	fmt.Printf("created table '%s'\n", tableName)

	// Insert data to database
	count := 0
	for i := 0; true; i++ {

		record, err := parser.Read()

		if nil != err {

			if io.EOF == err {

				count = i

				break
			}

			fmt.Printf("error parsing record: %s\n", strings.Join(record, ", "))
			fmt.Printf("error: %s\n", err.Error())
		}

		err = dbWriter.Insert(record, schema, tableName)

		if nil != err {

			fmt.Printf("error inserting record: %s\n", strings.Join(record, ", "))
			fmt.Printf("error: %s\n", err.Error())
		}
	}

	fmt.Printf("processed %d records\n", count)

	os.Exit(0)
}

func usage(){

	println("Usage:")
	println("make")
	println("./bin/csv-2-postgresql ./input_file.csv postgres://postgres:hello@localhost/test?sslmode=disable")
}