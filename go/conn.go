package openmldb

import (
	"bytes"
	"context"
	interfaces "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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

	_ interfaces.Rows = (*respDataRows)(nil)
)

type queryMode string

func (m queryMode) String() string {
	switch m {
	case ModeOffsync:
		return "offsync"
	case ModeOffasync:
		return "offasync"
	case ModeOnline:
		return "online"
	default:
		return "unknown"
	}
}

const (
	ModeOffsync  queryMode = "offsync"
	ModeOffasync queryMode = "offasync"
	ModeOnline   queryMode = "online"
)

var allQueryMode = map[string]queryMode{
	"offsync":  ModeOffsync,
	"offasync": ModeOffasync,
	"online":   ModeOnline,
}

type conn struct {
	host   string // host or host:port
	db     string // database name
	mode   queryMode
	closed bool
}

type queryResp struct {
	Code int       `json:"code"`
	Msg  string    `json:"msg"`
	Data *respData `json:"data,omitempty"`
}

type respData struct {
	Schema []string             `json:"schema"`
	Data   [][]interfaces.Value `json:"data"`
}

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
	if r.i >= len(r.Data) {
		return io.EOF
	}

	copy(dest, r.Data[r.i])
	r.i++
	return nil
}

type queryReq struct {
	Mode  string      `json:"mode"`
	SQL   string      `json:"sql"`
	Input *queryInput `json:"input,omitempty"`
}

type queryInput struct {
	Schema []string           `json:"schema"`
	Data   []interfaces.Value `json:"data"`
}

func parseReqToJson(mode, sql string, input ...interfaces.Value) ([]byte, error) {
	req := queryReq{
		Mode: mode,
		SQL:  sql,
	}

	if len(input) > 0 {
		schema := make([]string, len(input))
		for i, v := range input {
			switch v.(type) {
			case bool:
				schema[i] = "bool"
			case int16:
				schema[i] = "int16"
			case int32:
				schema[i] = "int32"
			case int64:
				schema[i] = "int64"
			case float32:
				schema[i] = "float"
			case float64:
				schema[i] = "double"
			case string:
				schema[i] = "string"
			default:
				return nil, fmt.Errorf("unknown type at index %d", i)
			}
		}
		req.Input = &queryInput{
			Schema: schema,
			Data:   input,
		}
	}

	return json.Marshal(req)
}

func parseRespFromJson(respBody io.Reader) (*queryResp, error) {
	var r queryResp
	if err := json.NewDecoder(respBody).Decode(&r); err != nil {
		return nil, err
	}

	if r.Data != nil {
		for _, row := range r.Data.Data {
			for i, col := range row {
				switch strings.ToLower(r.Data.Schema[i]) {
				case "bool":
					row[i] = col.(bool)
				case "int16":
					row[i] = int16(col.(float64))
				case "int32":
					row[i] = int32(col.(float64))
				case "int64":
					row[i] = int64(col.(float64))
				case "float":
					row[i] = float32(col.(float64))
				case "double":
					row[i] = float64(col.(float64))
				case "string":
					row[i] = col.(string)
				default:
					return nil, fmt.Errorf("unknown type %s at index %d", r.Data.Schema[i], i)
				}
			}
		}
	}

	return &r, nil
}

func (c *conn) query(ctx context.Context, sql string, parameters ...interfaces.Value) (rows interfaces.Rows, err error) {
	if c.closed {
		return nil, interfaces.ErrBadConn
	}

	reqBody, err := parseReqToJson(string(c.mode), sql, parameters...)
	if err != nil {
		return nil, err
	}

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

	if r, err := parseRespFromJson(resp.Body); err != nil {
		return nil, err
	} else if r.Code != 0 {
		return nil, fmt.Errorf("conn error: %s", r.Msg)
	} else if r.Data != nil {
		return &respDataRows{*r.Data, 0}, nil
	}

	return nil, nil
}

// Prepare implements driver.Conn.
func (c *conn) Prepare(query string) (interfaces.Stmt, error) {
	return nil, errors.New("Prepare is not implemented, use QueryContext instead")
}

// Close implements driver.Conn.
func (c *conn) Close() error {
	c.closed = true
	return nil
}

// Begin implements driver.Conn.
func (c *conn) Begin() (interfaces.Tx, error) {
	return nil, errors.New("begin not implemented")
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
	parameters := make([]interfaces.Value, len(args))
	for i, arg := range args {
		parameters[i] = arg.Value
	}
	if _, err := c.query(ctx, query, parameters...); err != nil {
		return nil, err
	}
	return interfaces.ResultNoRows, nil
}

// QueryContext implements driver.QueryerContext.
func (c *conn) QueryContext(ctx context.Context, query string, args []interfaces.NamedValue) (interfaces.Rows, error) {
	parameters := make([]interfaces.Value, len(args))
	for i, arg := range args {
		parameters[i] = arg.Value
	}
	return c.query(ctx, query, parameters...)
}
