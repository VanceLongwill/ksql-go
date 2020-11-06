package stdlib

import (
	"context"
	"database/sql/driver"

	"github.com/vancelongwill/ksql"
)

type Connector struct {
	client ksql.Client
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &Conn{client: &c.client}, nil
}

func (c *Connector) Driver() driver.Driver {
	return &Driver{}
}

func (c *Connector) Client() ksql.Client {
	return c.client
}

var _ driver.Connector = &Connector{}
