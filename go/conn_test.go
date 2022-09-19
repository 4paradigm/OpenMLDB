package openmldb

import (
	interfaces "database/sql/driver"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRespJson(t *testing.T) {
	for _, tc := range []struct {
		resp   string
		expect GeneralResp
	}{
		{
			`{
				"code": 0,
				"msg": "ok"
			}`,
			GeneralResp{
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
			GeneralResp{
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
			GeneralResp{
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
		actual, err := parseRespJson(strings.NewReader(tc.resp))
		assert.NoError(t, err)
		assert.Equal(t, &tc.expect, actual)
	}
}
