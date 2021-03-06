package stdlib

import (
	"database/sql/driver"

	ksql "github.com/vancelongwill/ksql-go/client"
)

// Driver is a database/sql compatible driver
type Driver struct{}

// Open returns a new connection with a new client
func (d *Driver) Open(url string) (driver.Conn, error) {
	return newConn(ksql.New(url)), nil
}

// OpenConnector returns a new connection with a new client
func (d *Driver) OpenConnector(url string) (driver.Connector, error) {
	return newConn(ksql.New(url)), nil
}

var _ driver.Driver = &Driver{}
var _ driver.DriverContext = &Driver{}
