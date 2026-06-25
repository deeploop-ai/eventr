.PHONY: build test validate tidy

BINARY := bin/eventr

build:
	go build -o $(BINARY) ./cmd/eventr

test:
	go test ./...

tidy:
	go mod tidy

validate: build
	./$(BINARY) validate --config testdata/pipelines/linear.yaml
