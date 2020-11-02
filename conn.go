package ksql

import "database/sql/driver"

type Conn struct {
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	panic("not implemented")
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	panic("not implemented")

}

var _ driver.Conn = &Conn{}
