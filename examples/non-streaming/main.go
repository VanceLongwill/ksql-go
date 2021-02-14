package main

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/vancelongwill/ksql-go/examples/seeder"
	_ "github.com/vancelongwill/ksql-go/stdlib"
	"log"
)

type rowResult struct {
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
