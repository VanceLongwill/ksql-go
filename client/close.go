package client

// Close gracefully closes all open connections in order to reuse TCP connections via keep-alive
func (c *Client) Close() error {
	for _, rows := range c.rows {
		if rows == nil {
			continue
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	for _, wtr := range c.insertsStreamWriters {
		if wtr == nil {
			continue
		}
		if err := wtr.Close(); err != nil {
			return err
		}
	}
	return nil
}
