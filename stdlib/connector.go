package stdlib

import (
	"context"
	"database/sql/driver"

	ksql "github.com/vancelongwill/ksql-go/client"
)

// Connector implements the database/sql/driver package's Connector interface
type Connector struct {
	client ksql.Client
}

// Connect returns a new connection with access to the client
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return newConn(c.client), nil
}

// Driver returns a new driver instance
func (c *Connector) Driver() driver.Driver {
	return &Driver{}
}

// Client returns the connector's ksql client
func (c *Connector) Client() ksql.Client {
	return c.client
}

// NewConnector allows a specific client to be passed to database/sql compatible connectors
func NewConnector(client ksql.Client) *Connector {
	return &Connector{client}
}

var _ driver.Connector = &Connector{}
