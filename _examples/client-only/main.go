package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/vancelongwill/ksql"
	"golang.org/x/sync/errgroup"
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

type DataRow struct {
	K  string `json:"k"`
	V1 int    `json:"v1"`
	V2 string `json:"v2"`
	V3 bool   `json:"v3"`
}

func run(ctx context.Context) error {
	db := ksql.New("http://0.0.0.0:8088/")

	info, err := db.Info(ctx)
	if err != nil {
		return err
	}
	log.Println(info)

	log.Println("Setting offset to earliest")
	_, err = db.Exec(ctx, ksql.ExecPayload{
		KSQL: "SET 'auto.offset.reset' = 'earliest';",
	})
	if err != nil {
		return err
	}
	log.Println("Creating stream")
	_, err = db.Exec(ctx, ksql.ExecPayload{
		KSQL: createStream,
		StreamsProperties: ksql.StreamsProperties{
			"auto.offset.reset": "earliest",
		},
	})
	if err != nil {
		return err

	}
	log.Println("Inserting data")
	_, err = db.Exec(ctx, ksql.ExecPayload{
		KSQL: insertData,
	})
	if err != nil {
		return err
	}
	log.Println("Creating table based on stream")
	_, err = db.Exec(ctx, ksql.ExecPayload{
		KSQL: createTable,
	})
	if err != nil {
		return err
	}

	g, _ := errgroup.WithContext(ctx)

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	g.Go(func() error {
		log.Println("Inserting via stream")
		wtr, err := db.InsertsStream(ctx, ksql.InsertsStreamTargetPayload{Target: "s1"})
		if err != nil {
			return fmt.Errorf("unable to open insert stream: %w", err)
		}
		defer wtr.Close()
		time.Sleep(2 * time.Second)
		dataRows := []DataRow{
			{K: strconv.Itoa(r1.Int()), V1: 99, V2: "yes", V3: true},
			{K: strconv.Itoa(r1.Int()), V1: 19292, V2: "asdasd", V3: false},
			{K: strconv.Itoa(r1.Int()), V1: 19292, V2: "asdasd", V3: false},
		}
		for _, r := range dataRows {
			time.Sleep(1 * time.Second)
			log.Printf("Writing item %#v", r)
			if err := wtr.WriteJSON(context.Background(), &r); err != nil {
				return fmt.Errorf("unable to write item %#v to stream: %w", r, err)
			}
		}
		return nil
	})

	g.Go(func() error {
		log.Println("Querying table")
		rows, err := db.QueryStream(ctx, ksql.QueryStreamPayload{
			KSQL: "SELECT * FROM t1 WHERE v1 > -1 EMIT CHANGES;",
		})
		if err != nil {
			return err
		}
		defer rows.Close()

		log.Println("Streaming results")
		dest := make([]interface{}, 4)
		for {
			err = rows.Next(dest)
			if err != nil {
				break
			}
			log.Printf("Received: %v", dest)
		}
		if err != nil && !errors.Is(err, ksql.ErrRowsClosed) {
			return err
		}
		return nil
	})

	done := make(chan error)
	go func() {
		done <- g.Wait()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func main() {
	log.Println("Starting... press ctrl-C to gracefully exit")
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}

}
