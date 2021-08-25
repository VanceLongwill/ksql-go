.PHONY: lint test coverage build build-examples
.SILENT: lint test coverage build build-examples

lint:
	golint ./... && go vet ./...

test:
	go test -count=1 -race ./...

coverage:
	go test -cover -count=1 -race -covermode=atomic -coverprofile=coverage.txt ./...
	go tool cover -html=coverage.txt -o coverage.html

build:
	go build ./...

build-examples:
	find ./examples -type d -mindepth 1 | xargs -I {} bash -c 'printf "Building {}\n"; cd {}; go build -o /dev/null .; cd - &>/dev/null'

