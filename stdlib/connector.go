package stdlib

import (
	"context"
	"database/sql/driver"

	"github.com/vancelongwill/ksql"
)

// Connector implements the database/sql/driver package's Connector interface
type Connector struct {
	client ksql.Client
}

// Connect returns a new connection with access to the client
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &Conn{client: &c.client}, nil
}

// Driver returns a new driver instance
func (c *Connector) Driver() driver.Driver {
	return &Driver{}
}

// Client returns the connector's ksql client
func (c *Connector) Client() ksql.Client {
	return c.client
}

var _ driver.Connector = &Connector{}
