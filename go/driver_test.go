package openmldb_test

import (
	// register openmldb driver
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/4paradigm/OpenMLDB/go/openmldb"
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

	if _, err = db.ExecContext(ctx, "create database db;"); err != nil {
		t.Errorf("fail to execute create database: %s", err)
	}
}
