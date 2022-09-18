package openmldb

import (
	"context"
	"database/sql"
	interfaces "database/sql/driver"
)

func init() {
	sql.Register("openmldb", &driver{})
}

var (
	_ interfaces.Driver        = (*driver)(nil)
	_ interfaces.Connector     = (*driver)(nil)
	_ interfaces.DriverContext = (*driver)(nil)
)

type driver struct {
}

// Open implements driver.Driver.
func (d *driver) Open(name string) (interfaces.Conn, error) {
	return &conn{}, nil
}

// Connect implements driver.Connector.
func (d *driver) Connect(ctx context.Context) (interfaces.Conn, error) {
	return &conn{}, nil
}

// Driver implements driver.Connector.
func (d *driver) Driver() interfaces.Driver {
	return d
}

// OpenConnector implements driver.DriverContext.
func (d *driver) OpenConnector(name string) (interfaces.Connector, error) {
	return d, nil
}
