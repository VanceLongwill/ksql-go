package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	ksql "github.com/vancelongwill/ksql/client"
	_ "github.com/vancelongwill/ksql/stdlib"

	"github.com/vancelongwill/ksql/examples/seeder"
)

func run(ctx context.Context) error {
	db, err := sqlx.Open("ksqldb", "http://0.0.0.0:8088/")
	if err != nil {
		return err
	}
	if err := seeder.New(db.DB).Seed(ctx); err != nil {
		return err
	}
	log.Println("Querying")
	rows, err := db.QueryxContext(ctx, "SELECT * FROM t1 WHERE v1 > -1 EMIT CHANGES;",
		ksql.NewQueryConfig().Stream().WithProperties(ksql.OffsetEarliest))
	if err != nil {
		return err
	}
	defer rows.Close()
	type RowResult struct {
		K  string `db:"K"`
		V1 int    `db:"V1"`
		V2 string `db:"V2"`
		V3 bool   `db:"V3"`
	}
	log.Println("Streaming rows")
	r := RowResult{}

	// this will continue forever unless the context is cancelled, or rows.Close is called
	for rows.Next() {
		err := rows.StructScan(&r)
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
