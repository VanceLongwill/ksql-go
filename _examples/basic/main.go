package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/vancelongwill/ksql/client"
)

func run(ctx context.Context) error {
	db, err := sql.Open("ksql", "http://0.0.0.0:8088/")
	if err != nil {
		return err
	}
	// create stream
	res, err := db.ExecContext(ctx, "")
	if err != nil {
		return err
	}

	res, err = db.ExecContext(ctx, "SELECT * FROM pageviews WHERE id = $1;", "some-id",
		client.StreamsProperties{
			"ksql.streams.auto.offset.reset": "earliest",
		})
	if err != nil {
		return err
	}
	// read stream
	return nil
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}

}
