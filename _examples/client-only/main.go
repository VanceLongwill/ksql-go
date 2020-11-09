package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vancelongwill/ksql"
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
	db := ksql.New("http://0.0.0.0:8088/")

	info, err := db.Info(ctx)
	if err != nil {
		return err
	}
	fmt.Println(info)

	fmt.Println("Setting offset to earliest")
	_, err = db.Exec(ctx, ksql.ExecPayload{
		KSQL: "SET 'auto.offset.reset' = 'earliest';",
	})
	if err != nil {
		return err
	}
	fmt.Println("Creating stream")
	_, err = db.Exec(ctx, ksql.ExecPayload{
		KSQL: createStream,
		StreamsProperties: ksql.StreamsProperties{
			"auto.offset.reset": "earliest",
		},
	})
	if err != nil {
		return err

	}
	// fmt.Println("Inserting data")
	// _, err = db.Exec(ctx, ksql.ExecPayload{
	// 	KSQL: insertData,
	// })
	// if err != nil {
	// 	return err
	// }
	fmt.Println("Creating table based on stream")
	_, err = db.Exec(ctx, ksql.ExecPayload{
		KSQL: createTable,
	})
	if err != nil {
		return err
	}
	fmt.Println("Querying table")
	rows, err := db.QueryStream(ctx, ksql.QueryStreamPayload{
		KSQL: "SELECT * FROM t1 WHERE v1 > -1 EMIT CHANGES;",
		// Properties: map[string]string{
		// 	"auto.offset.reset": "earliest",
		// },
	})
	if err != nil {
		return err
	}
	go func() {
		time.Sleep(time.Second * 5)
		fmt.Println("Closing stream")
		err := rows.Close()
		if err != nil {
			fmt.Printf("Error closing: %v\n", err)
			return
		}
		fmt.Println("Closed successfully")
	}()

	fmt.Println("Streaming results")
	dest := make([]driver.Value, 4)
	for {
		err = rows.Next(dest)
		if err != nil {
			break
		}
		fmt.Println(dest)
	}
	if errors.Is(err, ksql.ErrRowsClosed) {
		fmt.Println("Done")
	} else {
		fmt.Println(err)
	}
	return nil
}

func main() {
	// json.NewEncoder(os.Stdout).Encode(ksql.QueryPayload{
	// 	KSQL: "SET \\'auto.offset.reset\\' = \\'earliest\\';",
	// })
	// log.Println("")
	// json.NewEncoder(os.Stdout).Encode(ksql.QueryPayload{
	// 	KSQL: `SET \'auto.offset.reset\' = 'earliest';`,
	// })
	log.Println("")
	ctx := context.Background()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}

}
