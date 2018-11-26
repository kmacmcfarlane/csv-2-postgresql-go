package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/iancoleman/strcase"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/csv"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/postgresql"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {

	// Parse arguments
	args := flag.Args()

	if len(args) != 1 || len(args) != 2 {
		usage()
		os.Exit(1)
	}

	inputFile := args[0]


	var databaseName string
	if len(args) == 2 {
		databaseName = args[1]
	} else {
		databaseName = "test"
	}

	// Connect to DB
	connectionString := fmt.Sprintf("user=postgres dbname=%s sslmode=verify-full", databaseName)

	db, err := sql.Open("postgres", connectionString)

	if nil != err {
		panic(err)
	}

	defer db.Close()

	sqlWrapper := postgresql.NewSQLWrapper(db)

	dbWriter := postgresql.NewWriter(sqlWrapper)

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
	tableName := strcase.ToSnake(filenameAlone)

	err = dbWriter.CreateTable(tableName, schema)

	if nil != err {
		panic(fmt.Sprintf("error creating db table: %s", err.Error()))
	}

	fmt.Printf("created table '%s' in database %s", tableName, databaseName)

	// Insert data to database
	for {

		record, err := parser.Read()

		if nil != err {

			if io.EOF == err {
				break
			}

			fmt.Printf("error parsing record: %s", strings.Join(record, ", "))
			fmt.Printf("error: %s", err.Error())
		}

		err = dbWriter.Insert(record, schema, tableName)

		if nil != err {

			fmt.Printf("error inserting record: %s", strings.Join(record, ", "))
			fmt.Printf("error: %s", err.Error())
		}
	}

	println("import ")

	os.Exit(0)
}

func usage(){

	println("Usage:")
	println("csv-2-postgresql input_file.csv [database_name]")
}