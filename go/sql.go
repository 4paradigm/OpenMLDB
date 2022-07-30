package openmldb

import (
	interfaces "database/sql/driver"
	"errors"
)

var (
	_ interfaces.Stmt = (*stmt)(nil)

	_ interfaces.Result = (*result)(nil)

	_ interfaces.Rows = (*rows)(nil)

	_ interfaces.Tx = (*tx)(nil)
)

type stmt struct {
}

// Close implements driver.Stmt.
func (s *stmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt.
func (s *stmt) NumInput() int {
	return 0
}

// Exec implements driver.Stmt.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *stmt) Exec(args []interfaces.Value) (interfaces.Result, error) {
	return nil, errors.New("deprecated: Drivers should implement StmtExecContext instead (or additionally)")
}

// Query implements driver.Stmt.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *stmt) Query(args []interfaces.Value) (interfaces.Rows, error) {
	return nil, errors.New("deprecated: Drivers should implement StmtQueryContext instead (or additionally)")
}

type result struct {
}

// LastInsertId implements driver.Result.
func (r *result) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected implements driver.Result.
func (r *result) RowsAffected() (int64, error) {
	return 0, nil
}

type rows struct {
}

// Columns implements driver.Rows.
func (r *rows) Columns() []string {
	return nil
}

// Close implements driver.Rows.
func (r *rows) Close() error {
	return nil
}

// Next implements driver.Rows.s
func (r *rows) Next(dest []interfaces.Value) error {
	return nil
}

type tx struct {
}

// Commit implements driver.Tx.
func (t *tx) Commit() error {
	return nil
}

// Rollback implements driver.Tx.
func (t *tx) Rollback() error {
	return nil
}
