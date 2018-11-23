.DEFAULT_GOAL := all

all: |build test

test_deps:
	go get -u github.com/onsi/ginkgo/ginkgo
	go get -u github.com/onsi/gomega/...

build:
	go build -o ./bin/csv-2-postgresql ./csv-2-postgresql

test:
	ginkgo -r ./