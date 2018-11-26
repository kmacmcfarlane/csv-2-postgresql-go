package postgresql_test

import (
	"fmt"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/mock"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/postgresql"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/schema"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go/types"
)

var _ = Describe("Postgresql Database Adapter", func() {

	var (
		databaseWriter postgresql.Writer
		sql *mocks.SQL
	)

	BeforeEach(func(){
		sql = new(mocks.SQL)
	})

	AfterEach(func(){
		sql.AssertExpectations(GinkgoT())
	})

	Describe("Create Table", func(){

		Context("Small integer column", func(){
			It("Creates a table with a single SMALLINT column", func(){

				sql.
					On("Exec", fmt.Sprintf(`DROP TABLE IF EXISTS "tablename";`)).
					Return(new(mocks.Result), nil)

				sql.
					On("Exec",
`CREATE TABLE "tablename" (
"total" SMALLINT NOT NULL
);`).
					Return(new(mocks.Result), nil)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Int8,
							Name: "total"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.CreateTable("tablename", schema)

				Ω(err).Should(BeNil())
			})
		})

		Context("int16 integer column", func(){
			It("Creates a table with a single SMALLINT column", func(){

				sql.
					On("Exec", fmt.Sprintf(`DROP TABLE IF EXISTS "tablename";`)).
					Return(new(mocks.Result), nil)

				sql.On("Exec",
`CREATE TABLE "tablename" (
"total" SMALLINT NOT NULL
);`).
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Int16,
							Name: "total"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.CreateTable("tablename", schema)

				Ω(err).Should(BeNil())
			})
		})

		Context("int32 integer column", func(){
			It("Creates a table with a single INTEGER column", func(){

				sql.
					On("Exec", fmt.Sprintf(`DROP TABLE IF EXISTS "tablename";`)).
					Return(new(mocks.Result), nil)

				sql.On("Exec",
`CREATE TABLE "tablename" (
"total" INTEGER NOT NULL
);`).
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Int32,
							Name: "total"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.CreateTable("tablename", schema)

				Ω(err).Should(BeNil())
			})
		})

		Context("int64 integer column", func(){
			It("Creates a table with a single BIGINT column", func(){

				sql.
					On("Exec", fmt.Sprintf(`DROP TABLE IF EXISTS "tablename";`)).
					Return(new(mocks.Result), nil)

				sql.On("Exec",
`CREATE TABLE "tablename" (
"total" BIGINT NOT NULL
);`).
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Int64,
							Name: "total"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.CreateTable("tablename", schema)

				Ω(err).Should(BeNil())
			})
		})

		Context("float32 column", func(){
			It("Creates a table with a single FLOAT column", func(){

				sql.
					On("Exec", fmt.Sprintf(`DROP TABLE IF EXISTS "tablename";`)).
					Return(new(mocks.Result), nil)

				sql.On("Exec",
`CREATE TABLE "tablename" (
"total" FLOAT NOT NULL
);`).
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Float32,
							Name: "total"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.CreateTable("tablename", schema)

				Ω(err).Should(BeNil())
			})
		})

		Context("float64 column", func(){
			It("Creates a table with a single DOUBLE column", func(){

				sql.
					On("Exec", fmt.Sprintf(`DROP TABLE IF EXISTS "tablename";`)).
					Return(new(mocks.Result), nil)

				sql.On("Exec",
`CREATE TABLE "tablename" (
"total" DOUBLE NOT NULL
);`).
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Float64,
							Name: "total"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.CreateTable("tablename", schema)

				Ω(err).Should(BeNil())
			})
		})

		Context("string column", func(){
			It("Creates a table with a single TEXT column", func(){

				sql.
					On("Exec", fmt.Sprintf(`DROP TABLE IF EXISTS "tablename";`)).
					Return(new(mocks.Result), nil)

				sql.On("Exec",
`CREATE TABLE "tablename" (
"comments" TEXT NOT NULL
);`).
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.String,
							Name: "comments"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.CreateTable("tablename", schema)

				Ω(err).Should(BeNil())
			})
		})

		Context("Multiple columns", func(){
			It("Creates a table with INTEGER, FLOAT, and TEXT columns", func(){

				sql.
					On("Exec", fmt.Sprintf(`DROP TABLE IF EXISTS "tablename";`)).
					Return(new(mocks.Result), nil)

				sql.On("Exec",
`CREATE TABLE "tablename" (
"count" INTEGER NOT NULL,
"total" FLOAT NOT NULL,
"comments" TEXT NOT NULL
);`).
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Int32,
							Name: "count"},
						{
							Kind: types.Float32,
							Name: "total"},
						{
							Kind: types.String,
							Name: "comments"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.CreateTable("tablename", schema)

				Ω(err).Should(BeNil())
			})
		})
	})

	Describe("Insert", func(){

		Context("int32", func(){
			It("Inserts an integer value", func(){

				sql.On("Exec",
					`INSERT INTO "tablename" VALUES ($1);`, "13").
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Int32,
							Name: "count"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.Insert([]string{"13"}, schema, "tablename")

				Ω(err).Should(BeNil())
			})
		})

		Context("int32", func(){
			It("Inserts a string value", func(){

				sql.On("Exec",
					`INSERT INTO "tablename" VALUES ($1);`, "hello world").
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.String,
							Name: "comment"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.Insert([]string{"hello world"}, schema, "tablename")

				Ω(err).Should(BeNil())
			})
		})

		Context("multiple values", func(){
			It("Inserts an int, float, and string value", func(){

				sql.On("Exec",
					`INSERT INTO "tablename" VALUES ($1, $2, $3);`, "13", "321.123", "hello world").
					Return(new(mocks.Result), nil).
					Times(1)

				schema := schema.Schema{
					Columns: []schema.Column{
						{
							Kind: types.Int32,
							Name: "count"},
						{
							Kind: types.Float32,
							Name: "total"},
						{
							Kind: types.String,
							Name: "comment"}}}

				databaseWriter = postgresql.NewWriter(sql)

				err := databaseWriter.Insert([]string{"13", "321.123", "hello world"}, schema, "tablename")

				Ω(err).Should(BeNil())
			})
		})
	})
})
