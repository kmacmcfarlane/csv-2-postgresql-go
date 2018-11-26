package csv_test

import (
	"fmt"
	"github.com/kmacmcfarlane/csv-2-postgresql-go/csv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go/types"
	"strings"
)

var _ = Describe("Parser", func() {


	Describe("CSVParser", func(){

		var (
			parser *csv.Parser
		)

		Describe("Parse a Line", func(){

			Context("Int8", func(){
				It("Returns a schema with an int8", func(){

					header := "total\n"
					line := "127\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Int8))
				})
			})

			Context("Negative Int8", func(){
				It("Returns a schema with a negative int8", func(){

					header := "total\n"
					line := "-128\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Int8))
				})
			})

			Context("Int16", func(){
				It("Returns a schema with an int16", func(){

					header := "total\n"
					line := "32767\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Int16))
				})
			})

			Context("Negative Int16", func(){
				It("Returns a schema with a negative int16", func(){

					header := "total\n"
					line := "-32768\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Int16))
				})
			})

			Context("Int32", func(){
				It("Returns a schema with an int32", func(){

					header := "total\n"
					line := "2147483647\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Int32))
				})
			})

			Context("Negative Int32", func(){
				It("Returns a schema with a negative int32", func(){

					header := "total\n"
					line := "-2147483648\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Int32))
				})
			})

			Context("Int64", func(){
				It("Returns a schema with an int64", func(){

					header := "total\n"
					line := "2147483648\n" // one more than the max 32 bit signed int

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Int64))
				})
			})

			Context("Negative Int64", func(){
				It("Returns a schema with a negative int64", func(){

					header := "total\n"
					line := "-2147483649\n" // one less than the min 32 bit signed int

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Int64))
				})
			})

			Context("Float", func(){
				It("Returns a schema with a float", func(){

					header := "total\n"
					line := "123.321\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Float32))
				})
			})

			Context("Float32", func(){
				It("Returns a schema with a float32", func(){

					header := "total\n"
					line := "123.321\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Float32))
				})
			})

			Context("Negative Float32", func(){
				It("Returns a schema with a negative float32", func(){

					header := "total\n"
					line := "-123.321\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Float32))
				})
			})

			Context("Float64", func(){
				It("Returns a schema with a negative float32", func(){

					header := "total\n"
					line := "3.402824e+38\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Float64))
				})
			})

			Context("Negative Float64", func(){
				It("Returns a schema with a negative float64", func(){

					header := "total\n"
					line := "-3.402824e+38\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("total"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Float64))
				})
			})

			Context("Bool true", func(){
				It("Returns a schema with a bool", func(){

					header := "isFoo\n"
					line := "true\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("isFoo"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Bool))
				})
			})

			Context("Bool false", func(){
				It("Returns a schema with a bool", func(){

					header := "isFoo\n"
					line := "false\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("isFoo"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Bool))
				})
			})

			Context("Bool false", func(){
				It("Returns a schema with a bool", func(){

					header := "isFoo\n"
					line := "False\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(1))
					Ω(result.Columns[0].Name).Should(Equal("isFoo"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Bool))
				})
			})

			Context("More than one column", func(){
				It("Returns a schema with two columns", func(){

					header := "isFoo,total\n"
					line := "False,123.321\n"

					parser = CreateParser(header, line)

					result, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					Ω(len(result.Columns)).Should(Equal(2))

					Ω(result.Columns[0].Name).Should(Equal("isFoo"))
					Ω(result.Columns[0].Kind).Should(Equal(types.Bool))

					Ω(result.Columns[1].Name).Should(Equal("total"))
					Ω(result.Columns[1].Kind).Should(Equal(types.Float32))
				})
			})
		})

		Describe("Read", func(){
			Context("Read the cached line after schema parse", func(){
				It("Returns the cached line, then the second line", func(){

					header := "isFoo,total\n"
					line := "False,123.321\ntrue,500.005\n"

					parser = CreateParser(header, line)

					_, err := parser.ParseSchema()

					Ω(err).Should(BeNil())

					result1, err := parser.Read()

					Ω(err).Should(BeNil())

					Ω(len(result1)).Should(Equal(2))
					Ω(result1[0]).Should(Equal("False"))
					Ω(result1[1]).Should(Equal("123.321"))

					result2, err := parser.Read()

					Ω(err).Should(BeNil())

					Ω(len(result2)).Should(Equal(2))
					Ω(result2[0]).Should(Equal("true"))
					Ω(result2[1]).Should(Equal("500.005"))
				})
			})

			Context("Read a line before schema is parsed", func(){
				It("Returns an error", func(){

					header := "isFoo,total\n"
					line := "False,123.321\ntrue,500.005\n"

					parser = CreateParser(header, line)

					_, err := parser.Read()

					Ω(err).ShouldNot(BeNil())
				})
			})
		})
	})
})


func CreateParser(header string, line string) *csv.Parser {

	combined := fmt.Sprintf("%s%s", header, line)

	reader := strings.NewReader(combined)

	return csv.NewParser(reader)
}