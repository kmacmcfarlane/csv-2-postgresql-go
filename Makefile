.DEFAULT_GOAL := all

all: |build_deps test_deps build test

build_deps:
	go get github.com/lib/pq
	go get github.com/iancoleman/strcase

test_deps:
	go get -u github.com/onsi/ginkgo/ginkgo
	go get -u github.com/onsi/gomega/...
	go get github.com/vektra/mockery/.../

build:
	go build -o ./bin/csv-2-postgresql ./csv-2-postgresql

build_mocks:
	@mockery -all -dir postgresql/ -output mock -case=underscore

test: build_mocks
	ginkgo -r ./