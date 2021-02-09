package main

import (
	"context"
	"database/sql"
	"log"
	"github.com/jmoiron/sqlx"
	"github.com/vancelongwill/ksql-go/examples/seeder"
	_ "github.com/vancelongwill/ksql-go/stdlib"
)

type rowResult struct {
	K  sql.NullString `db:"K"`
	V1 sql.NullInt64  `db:"V1"`
	V2 sql.NullString `db:"V2"`
	V3 sql.NullBool   `db:"V3"`
}

func run(ctx context.Context) error {
	db, err := sqlx.Open("ksqldb", "http://0.0.0.0:8088/")
	if err != nil {
		return err
	}
	if err := seeder.New(db.DB).Seed(ctx); err != nil {
		return err
	}
	log.Println("Getting matching row")
	var r rowResult
	err = db.GetContext(ctx, &r, "SELECT * FROM t1 WHERE k = 'k2';")
	if err != nil {
		return err
	}
	log.Println(r)
	return nil
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
	log.Println("Done")
}
