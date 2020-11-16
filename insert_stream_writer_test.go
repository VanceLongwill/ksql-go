package ksql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInsertStreamWriter(t *testing.T) {
	t.Run("Writes the json to the underlying writer", func(t *testing.T) {
		ctx := context.Background()
		b := &bytes.Buffer{}
		enc := json.NewEncoder(b)
		wtr := &InsertsStreamWriter{
			enc:    enc,
			ackMap: map[int64]string{0: "ok"},
			closer: ioutil.NopCloser(b),
		}
		defer wtr.Close()
		in := map[string]string{"test": "ok"}
		err := wtr.WriteJSON(ctx, &in)
		assert.NoError(t, err)
		var out map[string]string
		err = json.NewDecoder(b).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, in, out)
	})
	t.Run("Fails when there is no corresponding ACK received before the timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		b := &bytes.Buffer{}
		enc := json.NewEncoder(b)
		wtr := &InsertsStreamWriter{
			enc:    enc,
			closer: ioutil.NopCloser(b),
		}
		defer wtr.Close()
		in := map[string]string{"test": "ok"}
		err := wtr.WriteJSON(ctx, &in)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
		var out map[string]string
		err = json.NewDecoder(b).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, in, out)
	})
	t.Run("Succeedes when the corresponding ACK is received", func(t *testing.T) {
		ctx := context.Background()
		b := &bytes.Buffer{}
		enc := json.NewEncoder(b)
		ackCh := make(chan InsertsStreamAck)
		wtr := &InsertsStreamWriter{
			enc:    enc,
			ackMap: map[int64]string{},
			closer: ioutil.NopCloser(b),
			ackCh:  ackCh,
		}
		// send the corresponding ack 100 milliseconds later
		go func() {
			time.Sleep(100 * time.Millisecond)
			ackCh <- InsertsStreamAck{Status: "ok", Seq: 0}
		}()
		defer wtr.Close()
		in := map[string]string{"test": "ok"}
		err := wtr.WriteJSON(ctx, &in)
		assert.NoError(t, err)
		var out map[string]string
		err = json.NewDecoder(b).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, in, out)
	})
}
