package seeder

import (
	"context"
	"database/sql"
	"log"
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

// Seeder is used to seed some dummy data into the DB for use in other examples
type Seeder struct {
	db *sql.DB
}

// Seed creates the relevant tables and inserts the dummy data
func (s *Seeder) Seed(ctx context.Context) error {
	log.Println("Setting offset to earliest")
	if _, err := s.db.ExecContext(ctx, "SET 'auto.offset.reset' = 'earliest';"); err != nil {
		return err
	}
	log.Println("Creating stream")
	if _, err := s.db.ExecContext(ctx, createStream); err != nil {
		return err
	}
	log.Println("Inserting data")
	if _, err := s.db.ExecContext(ctx, insertData); err != nil {
		return err
	}
	log.Println("Creating table based on stream")
	if _, err := s.db.ExecContext(ctx, createTable); err != nil {
		return err
	}
	return nil
}

// New returns a new seeder for the given DB
func New(db *sql.DB) *Seeder {
	return &Seeder{db}
}
