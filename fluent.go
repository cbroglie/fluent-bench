package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

const (
	dialTimeout = 10 * time.Millisecond
)

type fluent struct {
	conn io.WriteCloser
	mu   sync.Mutex
}

func (f *fluent) connect() error {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:24224", dialTimeout)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.conn = conn
	f.mu.Unlock()
	return nil
}

func (f *fluent) sendMessage(m *message) error {
	if m.Time == 0 {
		m.Time = time.Now().Unix()
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
	if f.conn == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conn.Close()
	f.conn = nil
	return nil
}

type message struct {
	Tag    string
	Time   int64
	Record map[string]interface{}
}

func (m *message) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(m.Record)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("[\"%s\",%d,%s,null]", m.Tag, m.Time, data)), nil
}
