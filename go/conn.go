package openmldb

import (
	"context"
	interfaces "database/sql/driver"
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
}

// Prepare implements driver.Conn.
func (c *conn) Prepare(query string) (interfaces.Stmt, error) {
	return &stmt{}, nil
}

// Close implements driver.Conn.
func (c *conn) Close() error {
	return nil
}

// Begin implements driver.Conn.
func (c *conn) Begin() (interfaces.Tx, error) {
	return &tx{}, nil
}

// Ping implements driver.Pinger.
func (c *conn) Ping(ctx context.Context) error {
	return nil
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
	return true
}

// ExecContext implements driver.ExecerContext.
func (c *conn) ExecContext(ctx context.Context, query string, args []interfaces.NamedValue) (interfaces.Result, error) {
	return &result{}, nil
}

// QueryContext implements driver.QueryerContext.
func (c *conn) QueryContext(ctx context.Context, query string, args []interfaces.NamedValue) (interfaces.Rows, error) {
	return &rows{}, nil
}
