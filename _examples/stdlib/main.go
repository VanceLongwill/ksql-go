package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/vancelongwill/ksql"
	"github.com/vancelongwill/ksql/_examples/seeder"
	"github.com/vancelongwill/ksql/stdlib"
)

func run(ctx context.Context) error {
	db, err := sql.Open("ksqldb", "http://0.0.0.0:8088/")
	if err != nil {
		return err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}

	err = conn.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*stdlib.Conn).Client()
		log.Println("Running healthcheck")
		res, err := conn.Healthcheck(ctx)
		if err != nil {
			return err
		}
		log.Printf("Healthcheck results: %v", res)
		return nil
	})

	if err != nil {
		return err
	}

	if err := seeder.New(db).Seed(ctx); err != nil {
		return err
	}

	log.Println("Querying")

	rows, err := db.QueryContext(ctx,
		"SELECT * FROM t1 WHERE v1 > -1 EMIT CHANGES;",
		ksql.NewQueryConfig().Stream().WithProperties(ksql.OffsetEarliest))

	if err != nil {
		return err
	}

	defer rows.Close()

	type RowResult struct {
		K  string
		V1 int
		V2 string
		V3 bool
	}

	log.Println("Streaming rows")

	r := RowResult{}

	// this will continue forever unless the context is cancelled, or rows.Close is called
	for rows.Next() {
		err := rows.Scan(&r.K, &r.V1, &r.V2, &r.V3)
		if err != nil {
			return err
		}
		log.Println(r)
	}

	// check if we stopped looping because of the context expiring as expected
	if err := rows.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	return nil
}

var timeoutSeconds = 20

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	log.Printf("Closing in %d seconds", timeoutSeconds)
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
	log.Println("Done")
}
