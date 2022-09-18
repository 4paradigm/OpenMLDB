package openmldb_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	// register openmldb driver
	_ "github.com/4paradigm/OpenMLDB/go/openmldb"
	"github.com/stretchr/testify/assert"
)

var (
	OPENMLDB_APISERVER_HOST = os.Getenv("OPENMLDB_APISERVER_HOST")
	OPENMLDB_APISERVER_PORT = os.Getenv("OPENMLDB_APISERVER_PORT")
)

func Test_driver(t *testing.T) {
	db, err := sql.Open("openmldb", fmt.Sprintf("openmldb://%s:%s/test_db", OPENMLDB_APISERVER_HOST, OPENMLDB_APISERVER_PORT))
	if err != nil {
		t.Errorf("fail to open connect: %s", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("fail to close connection: %s", err)
		}
	}()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Errorf("fail to ping connect: %s", err)
	}

	createTableStmt := "CREATE TABLE demo(c1 int, c2 string);"
	if _, err := db.ExecContext(ctx, createTableStmt); err != nil {
		t.Errorf("fail to exec %s: %v", createTableStmt, err)
	}

	insertValueStmt := `INSERT INTO demo VALUES (1, "bb"), (2, "bb");`
	if _, err := db.ExecContext(ctx, insertValueStmt); err != nil {
		t.Errorf("fail to exec %s: %v", insertValueStmt, err)
	}

	queryStmt := `SELECT c1, c2 FROM demo`
	if rows, err := db.QueryContext(ctx, queryStmt); err != nil {
		t.Errorf("fail to exec %s: %v", queryStmt, err)
	} else {
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Errorf("column types not available: %v", err)
		}
		assert.Len(t, types, 2)
	}

	if _, err = db.ExecContext(ctx, "create database db;"); err != nil {
		t.Errorf("fail to execute create database: %s", err)
	}
}
