# Kafka ksqlDB driver and KSQL client

[![CircleCI](https://circleci.com/gh/VanceLongwill/ksql-go/tree/master.svg?style=svg)](https://circleci.com/gh/VanceLongwill/ksql-go/tree/master)
[![codecov](https://codecov.io/gh/VanceLongwill/ksql-go/branch/master/graph/badge.svg?token=H3V2EA886S)](https://codecov.io/gh/VanceLongwill/ksql-go)


[![Go Reference](https://pkg.go.dev/badge/github.com/vancelongwill/ksql-go.svg)](https://pkg.go.dev/github.com/vancelongwill/ksql-go)

An idiomatic KSQL go client with `database/sql` integrations.

This project is currently an unofficial ksqlDB client for Go until it reaches maturity.

Once maturity it reached, the plan is to integrate it into the ksqlDB codebase and give it official support.

The original design doc is [in the ksqlDB repository](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-44-ksqldb-golang-client.md).

This client has been developed with the goals outlined in the ksqlDB developer guide [here](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/contributing/).

## Getting started

The best place to start is in the [examples](./examples/) folder. It contains fully fledged examples for working with `database/sql`, [sqlx](https://github.com/jmoiron/sqlx), and the client directly.

**Querying a stream with [sqlx](https://github.com/jmoiron/sqlx)** (recommended)

```go
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
        
	ksql "github.com/vancelongwill/ksql-go/client"
	_ "github.com/vancelongwill/ksql-go/stdlib" # import the database/sql driver
)

type Item struct {
        K  string `db:"K"`
        V1 int    `db:"V1"`
        V2 string `db:"V2"`
        V3 bool   `db:"V3"`
}

func run(ctx context.Context) error {
	db, err := sqlx.Open("ksqldb", "http://0.0.0.0:8088/")
	if err != nil {
		return err
	}
	rows, err := db.QueryxContext(ctx, "SELECT * FROM t1 WHERE v1 > -1 EMIT CHANGES;")
	if err != nil {
		return err
	}
	defer rows.Close()
	// this will continue forever unless the context is cancelled, or rows.Close is called
	for rows.Next() {
                var r Item
		err := rows.StructScan(&r)
		if err != nil {
			return err
		}
		log.Println(r)
	}
	return rows.Err()
}
```

## Documentation

- [GoDoc](https://pkg.go.dev/github.com/vancelongwill/ksql-go)
- Runnable [examples](./examples/) in the `examples/` directory

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
