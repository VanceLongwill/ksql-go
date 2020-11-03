package client

import (
	"io"
	"io/ioutil"
)

type Emptier struct {
	body io.ReadCloser
}

func (b Emptier) Close() error {
	if _, err := io.Copy(ioutil.Discard, b.body); err != nil {
		return err
	}
	return b.Close()
}

func (b Emptier) Read(p []byte) (int, error) {
	return b.body.Read(p)
}
