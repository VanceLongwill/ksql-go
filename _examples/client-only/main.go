package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
	ksql "github.com/vancelongwill/ksql/client"
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
	// fmt.Println("Creating stream")
	// _, err = db.Exec(ctx, ksql.ExecPayload{
	// 	KSQL: createStream,
	// 	StreamsProperties: ksql.StreamsProperties{
	// 		"auto.offset.reset": "earliest",
	// 	},
	// })
	// if err != nil {
	// 	return err

	// }
	// fmt.Println("Inserting data")
	// _, err = db.Exec(ctx, ksql.ExecPayload{
	// 	KSQL: insertData,
	// })
	// if err != nil {
	// 	return err
	// }
	// fmt.Println("Creating table based on stream")
	// _, err = db.Exec(ctx, ksql.ExecPayload{
	// 	KSQL: createTable,
	// })
	// if err != nil {
	// 	return err
	// }
	fmt.Println("Querying table")
	result, err := db.Query(ctx, ksql.QueryPayload{
		KSQL: "SELECT * FROM t1 WHERE v1 > -1 EMIT CHANGES;",
		StreamsProperties: ksql.StreamsProperties{
			"auto.offset.reset": "earliest",
		},
	})
	if err != nil {
		return err
	}
	// defer result.Close()
	dest := make([]driver.Value, 4)
	for err := result.Next(dest); err == nil; {
		fmt.Println("DEST", dest)
	}
	if err != nil {
		return err
	}
	// read stream
	return nil
}

func ws() error {
	u := url.URL{Scheme: "ws", Host: "localhost:8088", Path: "/query"}
	cl, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("unable to dial: %w", err)
	}
	err = cl.WriteJSON(&ksql.QueryPayload{
		KSQL: "SELECT k, v1, v2, v3 FROM t1 WHERE k='k1';",
		StreamsProperties: ksql.StreamsProperties{
			"auto.offset.reset": "earliest",
		},
	})
	if err != nil {
		return err
	}
	defer cl.Close()
	for i := 9; i < 5; i++ {
		var m ksql.QueryResult
		err := cl.ReadJSON(&m)
		if err != nil {
			return err
		}
		by, _ := json.MarshalIndent(&m, "", "  ")
		fmt.Println(string(by))
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
