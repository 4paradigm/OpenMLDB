package openmldb

import (
	"bytes"
	"context"
	interfaces "database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
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
	Code int       `json:"code"`
	Msg  string    `json:"msg"`
	Data *respData `json:"data"`
}

type respData struct {
	Schema []string             `json:"schema"`
	Data   [][]interfaces.Value `json:"data"`
}

var _ interfaces.Rows = (*respDataRows)(nil)

type respDataRows struct {
	respData
	i int
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r respDataRows) Columns() []string {
	return make([]string, len(r.Schema))
}

// Close closes the rows iterator.
func (r *respDataRows) Close() error {
	r.i = len(r.Data)
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *respDataRows) Next(dest []interfaces.Value) error {
	if r.i < len(r.Data) {
		for i := 0; i < len(r.Data[r.i]); i++ {
			dest[i] = r.Data[i]
		}
		r.i++
		return nil
	}
	return io.EOF
}

type QueryReq struct {
	Mode  string `json:"mode"`
	SQL   string `json:"sql"`
	Input *queryInput
}

type queryInput struct {
	Schema []string           `json:"schema"`
	Data   []interfaces.Value `json:"data"`
}

func (c *conn) query(ctx context.Context, sql string, parameters ...interfaces.Value) (rows interfaces.Rows, err error) {
	if c.closed {
		return nil, interfaces.ErrBadConn
	}

	queryReq := QueryReq{
		Mode: "offsync",
		SQL:  sql,
	}

	if len(parameters) != 0 {
		var schema []string
		for _, parameter := range parameters {
			var typeName string
			switch parameter.(type) {
			case bool:
				typeName = "Bool"
			case int16:
				typeName = "Int16"
			case int32:
				typeName = "Int32"
			case int64:
				typeName = "Int64"
			case float32:
				typeName = "Float"
			case float64:
				typeName = "Double"
			case string:
				typeName = "String"
			}
			schema = append(schema, typeName)
		}
		queryReq.Input = &queryInput{
			Schema: schema,
			Data:   parameters,
		}
	}

	reqBody, _ := json.Marshal(queryReq)

	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		fmt.Sprintf("http://%s/dbs/%s", c.host, c.db),
		bytes.NewBuffer(reqBody),
	)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	var r GeneralResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("unexpected response: %w", err)
	}

	if r.Code != 0 {
		return nil, fmt.Errorf("conn error: %s", r.Msg)
	}

	if r.Data != nil {
		return &respDataRows{*r.Data, 0}, nil
	}

	return nil, nil
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
	_, err := c.query(ctx, "SELECT 1")
	return err
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
	_, err := c.query(ctx, query)
	return &result{}, err
}

// QueryContext implements driver.QueryerContext.
func (c *conn) QueryContext(ctx context.Context, query string, args []interfaces.NamedValue) (interfaces.Rows, error) {
	return c.query(ctx, query)
}
