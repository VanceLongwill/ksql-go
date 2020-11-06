package stdlib

import "database/sql"

func init() {
	sql.Register("ksqldb", &Driver{})
}
