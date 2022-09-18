package openmldb

import "testing"

func Test_parseDsn(t *testing.T) {
	for _, tc := range []struct {
		dsn  string
		host string
		db   string
		err  error
	}{
		{"openmldb://127.0.0.1:8080/test_db", "127.0.0.1:8080", "test_db", nil},
	} {
		host, db, err := parseDsn(tc.dsn)
		if err != tc.err {
			t.Errorf("Expect error: %e, but got: %s", tc.err, err)
		}
		if db != tc.db {
			t.Errorf("Expect db: %s, but got: %s", tc.db, db)
		}
		if host != tc.host {
			t.Errorf("Expect host: %s, but got: %s", tc.host, host)
		}
	}
}
