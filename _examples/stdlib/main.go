package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/vancelongwill/ksql"
	"github.com/vancelongwill/ksql/stdlib"
	_ "github.com/vancelongwill/ksql/stdlib" // driver
)

var (
	createTable = `
CREATE TABLE t1 AS
SELECT k,
       LATEST_BY_OFFSET(v1) AS v1,
       LATEST_BY_OFFSET(v2) AS v2,
       LATEST_BY_OFFSET(v3) AS v3
FROM s1
GROUP BY k
EMIT CHANGES;`
	createStream = `
CREATE STREAM s1 (
    k VARCHAR KEY,
    v1 INT,
    v2 VARCHAR,
    v3 BOOLEAN
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro'
);`
	insertData = `
INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k1', 0, 'a', true
);

INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k2', 1, 'b', false
);

INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k1', 2, 'c', false
);

INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k3', 3, 'd', true
);

INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k2', 4, 'e', true
);`
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

	log.Println("Creating stream")
	_, err = db.ExecContext(ctx, createStream)
	if err != nil {
		return err
	}

	log.Println("Inserting data")
	_, err = db.ExecContext(ctx, insertData)
	if err != nil {
		return err
	}

	log.Println("Creating table based on stream")
	_, err = db.ExecContext(ctx, createTable)
	if err != nil {
		return err
	}

	log.Println("Querying")
	rows, err := db.QueryContext(ctx,
		"SELECT * FROM t1 WHERE v1 > -1 EMIT CHANGES;",
		ksql.NewQueryConfig().Stream().WithProperties(ksql.OffsetEarliest),
	)
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
	err = rows.Err()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
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
