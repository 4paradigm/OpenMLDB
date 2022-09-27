package openmldb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseDsn(t *testing.T) {
	for _, tc := range []struct {
		dsn  string
		host string
		db   string
		mode queryMode
		err  error
	}{
		{"openmldb://127.0.0.1:8080/test_db", "127.0.0.1:8080", "test_db", ModeOffsync, nil},
		{"openmldb://127.0.0.1:8080/test_db?mode=online", "127.0.0.1:8080", "test_db", ModeOnline, nil},
		{"openmldb://127.0.0.1:8080/test_db?mode=offasync", "127.0.0.1:8080", "test_db", ModeOffasync, nil},
		{"openmldb://127.0.0.1:8080/test_db?mode=unknown", "127.0.0.1:8080", "test_db", "", errors.New("")},
	} {
		host, db, mode, err := parseDsn(tc.dsn)
		if tc.err == nil {
			assert.NoError(t, err)
			assert.Equal(t, host, tc.host)
			assert.Equal(t, db, tc.db)
			assert.Equal(t, mode, tc.mode)
		} else {
			assert.Error(t, err)
		}
	}
}
