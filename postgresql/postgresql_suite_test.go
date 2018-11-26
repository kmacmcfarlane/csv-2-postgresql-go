package postgresql_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPostgresql(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgresql Suite")
}
