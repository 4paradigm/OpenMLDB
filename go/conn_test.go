package openmldb

import (
	interfaces "database/sql/driver"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseReqToJson(t *testing.T) {
	for _, tc := range []struct {
		mode   string
		sql    string
		input  []interfaces.Value
		expect string
	}{
		{
			"offsync",
			"SELECT 1;",
			nil,
			`{
				"mode": "offsync",
				"sql": "SELECT 1;"
			}`,
		},
		{
			"offsync",
			"SELECT c1, c2 FROM demo WHERE c1 = ? AND c2 = ?;",
			[]interfaces.Value{int32(1), "bb"},
			`{
				"mode": "offsync",
				"sql": "SELECT c1, c2 FROM demo WHERE c1 = ? AND c2 = ?;",
				"input": {
					"schema": ["int32", "string"],
					"data": [1, "bb"]
				}
			}`,
		},
	} {
		actual, err := parseReqToJson(tc.mode, tc.sql, tc.input...)
		assert.NoError(t, err)
		assert.JSONEq(t, tc.expect, string(actual))
	}
}

func TestParseRespFromJson(t *testing.T) {
	for _, tc := range []struct {
		resp   string
		expect queryResp
	}{
		{
			`{
				"code": 0,
				"msg": "ok"
			}`,
			queryResp{
				Code: 0,
				Msg:  "ok",
				Data: nil,
			},
		},
		{
			`{
				"code": 0,
				"msg": "ok",
				"data": {
					"schema": ["Int32", "String"],
					"data": [[1, "bb"], [2, "bb"]]
				}
			}`,
			queryResp{
				Code: 0,
				Msg:  "ok",
				Data: &respData{
					Schema: []string{"Int32", "String"},
					Data: [][]interfaces.Value{
						{int32(1), "bb"},
						{int32(2), "bb"},
					},
				},
			},
		},
		{
			`{
				"code": 0,
				"msg": "ok",
				"data": {
					"schema": ["Bool", "Int16", "Int32", "Int64", "Float", "Double", "String"],
					"data": [[true, 1, 1, 1, 1, 1, "bb"]]
				}
			}`,
			queryResp{
				Code: 0,
				Msg:  "ok",
				Data: &respData{
					Schema: []string{"Bool", "Int16", "Int32", "Int64", "Float", "Double", "String"},
					Data: [][]interfaces.Value{
						{true, int16(1), int32(1), int64(1), float32(1), float64(1), "bb"},
					},
				},
			},
		},
	} {
		actual, err := parseRespFromJson(strings.NewReader(tc.resp))
		assert.NoError(t, err)
		assert.Equal(t, &tc.expect, actual)
	}
}
