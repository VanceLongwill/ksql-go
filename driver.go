package ksql

import "database/sql/driver"

type Driver struct{}

func (d *Driver) Open(url string) (driver.Conn, error) {
	return &Conn{}, nil
}

var _ driver.Driver = &Driver{}
