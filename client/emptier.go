package client

import (
	"io"
	"io/ioutil"
)

// Emptier discards the contents of a ReadCloser before closing. Used on http response bodies in order to keep connections alive.
type Emptier struct {
	body io.ReadCloser
}

// Close the ReadCloser but not before discarding its contents
func (b Emptier) Close() error {
	if _, err := io.Copy(ioutil.Discard, b.body); err != nil {
		return err
	}
	return b.Close()
}

// Read proxies the underlying ReadCloser's reader
func (b Emptier) Read(p []byte) (int, error) {
	return b.body.Read(p)
}
