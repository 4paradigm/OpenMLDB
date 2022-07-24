package openmldb

import (
	"context"
	"database/sql"
	interfaces "database/sql/driver"
	"fmt"
	"net/url"
	"strings"
)

func init() {
	sql.Register("openmldb", &driver{})
}

var (
	_ interfaces.Driver        = (*driver)(nil)
	_ interfaces.DriverContext = (*driver)(nil)

	_ interfaces.Connector = (*connecter)(nil)
)

type driver struct{}

func parseDsn(dsn string) (host string, db string, err error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", "", fmt.Errorf("invlaid URL: %w", err)
	}

	if u.Scheme != "openmldb" && u.Scheme != "" {
		return "", "", fmt.Errorf("invalid URL: unknown schema '%s'", u.Scheme)
	}

	p := strings.Split(u.Path, "/")
	if len(p) == 0 {
		return "", "", fmt.Errorf("invalid URL: DB name not found")
	}

	return u.Host, p[0], nil
}

// Open implements driver.Driver.
func (driver) Open(name string) (interfaces.Conn, error) {
	// name should be the URL of the api server, e.g. openmldb://localhost:6543/db
	host, db, err := parseDsn(name)
	if err != nil {
		return nil, err
	}

	return &conn{host: host, db: db, closed: false}, nil
}

type connecter struct {
	host string
	db   string
}

// Connect implements driver.Connector.
func (c connecter) Connect(ctx context.Context) (interfaces.Conn, error) {
	conn := &conn{host: c.host, db: c.db, closed: false}
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}

// Driver implements driver.Connector.
func (connecter) Driver() interfaces.Driver {
	return &driver{}
}

// OpenConnector implements driver.DriverContext.
func (driver) OpenConnector(name string) (interfaces.Connector, error) {
	host, db, err := parseDsn(name)
	if err != nil {
		return nil, err
	}

	return &connecter{host, db}, nil
}
