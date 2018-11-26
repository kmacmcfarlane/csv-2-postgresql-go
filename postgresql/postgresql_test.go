package postgresql_test

import (
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

	Describe("Create Table", func(){

		Context("Small integer column", func(){
			It("Creates a table with a single SMALLINT column", func(){

				sql.On("Exec",
`CREATE TABLE "tablename" (
_id UUID PRIMARY KEY,
"total" SMALLINT NOT NULL
);`).
					Return(new(mocks.Result), nil).
					Times(1)

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

				sql.On("Exec",
`CREATE TABLE "tablename" (
_id UUID PRIMARY KEY,
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

				sql.On("Exec",
`CREATE TABLE "tablename" (
_id UUID PRIMARY KEY,
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

				sql.On("Exec",
`CREATE TABLE "tablename" (
_id UUID PRIMARY KEY,
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

				sql.On("Exec",
`CREATE TABLE "tablename" (
_id UUID PRIMARY KEY,
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

				sql.On("Exec",
`CREATE TABLE "tablename" (
_id UUID PRIMARY KEY,
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

				sql.On("Exec",
`CREATE TABLE "tablename" (
_id UUID PRIMARY KEY,
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
	})
})
