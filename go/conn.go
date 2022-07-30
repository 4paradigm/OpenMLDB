package openmldb

import (
	"bytes"
	"context"
	interfaces "database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
)

var (
	_ interfaces.Conn = (*conn)(nil)

	// All Conn implementations should implement the following interfaces:
	// Pinger, SessionResetter, and Validator.

	_ interfaces.Pinger          = (*conn)(nil)
	_ interfaces.SessionResetter = (*conn)(nil)
	_ interfaces.Validator       = (*conn)(nil)

	// If named parameters or context are supported, the driver's Conn should implement:
	// ExecerContext, QueryerContext, ConnPrepareContext, and ConnBeginTx.

	_ interfaces.ExecerContext  = (*conn)(nil)
	_ interfaces.QueryerContext = (*conn)(nil)
)

type conn struct {
	host   string // host or host:port
	db     string // database name
	closed bool
}

type GeneralResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type QueryReq struct {
	Mode string `json:"mode"`
	SQL  string `json:"sql"`
}

func (c *conn) query(ctx context.Context, sql string) (err error) {
	// TODO: implement query entry return result
	if c.closed {
		return interfaces.ErrBadConn
	}

	reqBody, _ := json.Marshal(&QueryReq{
		Mode: "offsync",
		SQL:  sql,
	})

	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		fmt.Sprintf("http://%s/dbs/%s", c.host, c.db),
		bytes.NewBuffer(reqBody),
	)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	var r GeneralResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("unexpected response: %w", err)
	}

	if r.Code != 0 {
		return fmt.Errorf("conn error: %s", r.Msg)
	}

	return nil
}

// Prepare implements driver.Conn.
func (c *conn) Prepare(query string) (interfaces.Stmt, error) {
	return &stmt{}, nil
}

// Close implements driver.Conn.
func (c *conn) Close() error {
	c.closed = true
	return nil
}

// Begin implements driver.Conn.
func (c *conn) Begin() (interfaces.Tx, error) {
	return &tx{}, nil
}

// Ping implements driver.Pinger.
func (c *conn) Ping(ctx context.Context) error {
	return c.query(ctx, "SELECT 1")
}

// ResetSession implements driver.SessionResetter.
//
// Before a connection is reused for another query, ResetSession is called.
func (c *conn) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid implements driver.Validator.
//
// Before a connection is returned to the connection pool after use, IsValid is called.
func (c *conn) IsValid() bool {
	return !c.closed
}

// ExecContext implements driver.ExecerContext.
func (c *conn) ExecContext(ctx context.Context, query string, args []interfaces.NamedValue) (interfaces.Result, error) {
	return &result{}, c.query(ctx, query)
}

// QueryContext implements driver.QueryerContext.
func (c *conn) QueryContext(ctx context.Context, query string, args []interfaces.NamedValue) (interfaces.Rows, error) {
	return &rows{}, c.query(ctx, query)
}
