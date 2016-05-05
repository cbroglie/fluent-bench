package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/samuel/go-thrift/examples/scribe"
	"github.com/samuel/go-thrift/thrift"
	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	dialTimeout = 10 * time.Millisecond
)

type fluent struct {
	scribe *scribe.ScribeClient
	conn   io.WriteCloser
}

func (f *fluent) connectScribe() error {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:1463", dialTimeout)
	if err != nil {
		return err
	}

	t := thrift.NewTransport(thrift.NewFramedReadWriteCloser(conn, 0), thrift.BinaryProtocol)
	client := thrift.NewClient(t, false)
	f.scribe = &scribe.ScribeClient{Client: client}

	return nil
}

func (f *fluent) connectTCP() error {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:24224", dialTimeout)
	if err != nil {
		return err
	}
	f.conn = conn
	return nil
}

func (f *fluent) sendMessage(m *message) error {
	if m.Time == 0 {
		m.Time = time.Now().Unix()
	}

	if f.scribe != nil {
		b, err := json.Marshal(m.Record)
		if err != nil {
			return err
		}
		res, err := f.scribe.Log([]*scribe.LogEntry{{m.Tag, string(b)}})
		if err != nil {
			return err
		}
		if res != scribe.ResultCodeOk {
			return fmt.Errorf("fluent#sendMessage: send returned %s", res.String())
		}
		return nil
	}

	payload := []interface{}{m.Tag, []interface{}{[]interface{}{m.Time, m.Record}}}
	b, err := msgpack.Marshal(payload)
	if err != nil {
		return err
	}
	return f.send(b)
}

func (f *fluent) send(b []byte) error {
	if f.conn == nil {
		return errors.New("fluent#send: not connected")
	}
	_, err := f.conn.Write(b)
	return err
}

func (f *fluent) close() error {
	if f.conn != nil {
		f.conn.Close()
		f.conn = nil
	}
	return nil
}

type message struct {
	Tag    string
	Time   int64
	Record map[string]interface{}
}
