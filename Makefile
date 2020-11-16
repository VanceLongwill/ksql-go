.PHONY: lint test coverage build build-examples

lint:
	golint ./... && go vet ./...
test:
	go test -v ./...

coverage:
	go test -v -coverprofile coverage.out && go tool cover -html=coverage.out -o coverage.html

build:
	go build ./...

build-examples:
	find ./_examples -type d -mindepth 1 | xargs -I {} bash -c 'printf "Building {}"; go build -o /dev/null {}; printf "    Done\n"'

