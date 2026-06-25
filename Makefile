.PHONY: build test validate tidy pipeline-test

BINARY := bin/eventr

build:
	go build -o $(BINARY) ./cmd/eventr

test:
	go test ./...

tidy:
	go mod tidy

validate: build
	./$(BINARY) validate --config testdata/pipelines/linear.yaml

pipeline-test: build
	./$(BINARY) test --dir testdata/tests
