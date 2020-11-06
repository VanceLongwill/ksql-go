package stdlib

import (
	"database/sql/driver"

	"github.com/vancelongwill/ksql"
)

type Driver struct{}

func (d *Driver) Open(url string) (driver.Conn, error) {
	return &Conn{
		client: ksql.New(url),
	}, nil
}

func (d *Driver) OpenConnector(url string) (driver.Connector, error) {
	return &Conn{
		client: ksql.New(url),
	}, nil
}

var _ driver.Driver = &Driver{}
var _ driver.DriverContext = &Driver{}
