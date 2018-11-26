package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/postgresql"
	"os"
)

func main() {

	// Parse arguments
	args := flag.Args()

	if len(args) != 1 || len(args) != 2 {
		usage()
		os.Exit(1)
	}

	inputFile := args[0]
	databaseName := args[1]

	// Connect to DB
	connectionString := fmt.Sprintf("user=postgres dbname=%s sslmode=verify-full", databaseName)

	db, err := sql.Open("postgres", connectionString)

	if nil != err {
		panic(err)
	}

	defer db.Close()

	sqlWrapper := postgresql.NewSQLWrapper(db)

	// Parse schema of input

	// Insert data to database
	
}

func usage(){

	println("Usage:")
	println("csv-2-postgresql input_file.csv [database_name]")
}