# Kafka ksqlDB driver

[![CircleCI](https://circleci.com/gh/VanceLongwill/ksql-go/tree/master.svg?style=svg)](https://circleci.com/gh/VanceLongwill/ksql-go/tree/master)

[![codecov](https://codecov.io/gh/VanceLongwill/ksql-go/branch/master/graph/badge.svg?token=H3V2EA886S)](https://codecov.io/gh/VanceLongwill/ksql-go)

This project is currently an unofficial ksqlDB client until it reaches maturity.

Once maturity it reached, the plan is to integrate it into the ksqlDB codebase and give it official support.

The original design doc is [in the ksqlDB repository](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-44-ksqldb-golang-client.md).

This client has been developed with the goals outlined in the ksqlDB developer guide [here](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/contributing/).

## Documentation

- [GoDoc](https://pkg.go.dev/github.com/vancelongwill/ksql-go)
- User guide *(coming soon)*
- Runnable [examples](./examples/README.md) in the `examples/` directory

## Features

- Compatible with the `database/sql` driver
- Supports all the features of the ksqlDB REST API
- Provides high level API for working with pull & pull queries


## Developing

1. Pull the repo
2. Get the dependencies
```sh
go mod download
```
3. Run all unit tests and generate a coverage report
```sh
make coverage
```

## Testing

At the moment the primary focus is on unit testing although there are plans to add some integration tests based on the [examples](./examples/README.md).

## TODO:

- [x] Support all ksqlDB REST API methods
- [x] TLS support (use custom http client)
- [x] More examples
- [x] GoDoc
- [x] Passes golint & go vet
- [ ] More tests (see coverage reports)
- [ ] User documentation
- [ ] More semantic error handling
- [ ] Handle HTTP error codes
