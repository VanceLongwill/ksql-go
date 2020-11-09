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
		b := &bytes.Buffer{}
		r := ioutil.NopCloser(&bytes.Reader{})
		wtr := newInsertStreamWriter(b, r)
		defer wtr.Close()
		in := map[string]string{"test": "ok"}
		err := wtr.WriteJSON(&in)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
		var out map[string]string
		err = json.NewDecoder(b).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, in, out)
	})
	t.Run("Fails when there is no corresponding ACK received before the timeout", func(t *testing.T) {
		b := &bytes.Buffer{}
		r := ioutil.NopCloser(&bytes.Reader{})
		wtr := newInsertStreamWriter(b, r)
		defer wtr.Close()
		in := map[string]string{"test": "ok"}
		err := wtr.WriteJSON(&in)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
		var out map[string]string
		err = json.NewDecoder(b).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, in, out)
	})
	t.Run("Reads an ack", func(t *testing.T) {
		b := &bytes.Buffer{}
		w := &bytes.Buffer{}
		r := ioutil.NopCloser(w)
		wtr := newInsertStreamWriter(b, r)
		defer wtr.Close()
		ack1 := InsertsStreamAck{Status: "ok", Seq: 0}
		if err := json.NewEncoder(w).Encode(&ack1); err != nil {
			t.Fatal(err)
		}
		err := wtr.readAck()
		assert.NoError(t, err)
		assert.Equal(t, ack1.Status, wtr.acks[0])
	})
	t.Run("Reads an ack async", func(t *testing.T) {
		b := &bytes.Buffer{}
		w := &bytes.Buffer{}
		r := ioutil.NopCloser(w)
		wtr := newInsertStreamWriter(b, r)
		defer wtr.Close()
		ack1 := InsertsStreamAck{Status: "ok", Seq: 0}
		err := wtr.readAck()
		assert.NoError(t, err)
		if err := json.NewEncoder(w).Encode(&ack1); err != nil {
			t.Fatal(err)
		}
		err = wtr.readAck()
		assert.NoError(t, err)
		got, ok := wtr.acks[0]
		assert.True(t, ok)
		assert.Equal(t, ack1.Status, got)
	})
	t.Run("Succeedes when the corresponding ACK is received", func(t *testing.T) {
		b := &bytes.Buffer{}
		w := &bytes.Buffer{}
		r := ioutil.NopCloser(w)
		wtr := newInsertStreamWriter(b, r)
		// send the corresponding ack 1 second later
		go func() {
			time.Sleep(1 * time.Second)
			ack1 := InsertsStreamAck{Status: "ok", Seq: 0}
			if err := json.NewEncoder(w).Encode(&ack1); err != nil {
				panic(err)
			}
		}()
		defer wtr.Close()
		in := map[string]string{"test": "ok"}
		err := wtr.WriteJSON(&in)
		assert.NoError(t, err)
		var out map[string]string
		err = json.NewDecoder(b).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, in, out)
	})
	t.Run("Fails by timeing out when an ack is received but is not the correct sequence", func(t *testing.T) {
		b := &bytes.Buffer{}
		w := &bytes.Buffer{}
		r := ioutil.NopCloser(w)
		// send the corresponding ack 1 second later
		go func() {
			time.Sleep(1 * time.Second)
			ack1 := InsertsStreamAck{Status: "ok", Seq: 6}
			if err := json.NewEncoder(w).Encode(&ack1); err != nil {
				t.Fatalf("unexpected error sending JSON ack in test: %v", err)
			}
		}()
		wtr := newInsertStreamWriter(b, r)
		defer wtr.Close()
		in := map[string]string{"test": "ok"}
		err := wtr.WriteJSON(&in)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
		var out map[string]string
		err = json.NewDecoder(b).Decode(&out)
		assert.NoError(t, err)
		assert.Equal(t, in, out)
	})
}
