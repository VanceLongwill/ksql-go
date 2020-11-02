package ksql

import "database/sql"

func init() {
	sql.Register("ksqldb", &Driver{})
}
