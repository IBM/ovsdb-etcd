package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseKey(t *testing.T) {
	tests := []struct {
		keyStr string
		prefix string
		expKey *Key
		expErr error
	}{
		{keyStr: "/table/id", expErr: fmt.Errorf("wrong formatted key")},
		{keyStr: "table/id", expErr: fmt.Errorf("wrong formatted key")},
		{keyStr: "db/table/", expErr: fmt.Errorf("wrong formatted key")},
		{keyStr: "ovsdb/nb/table/id/", prefix: "ovsdb/nb", expErr: fmt.Errorf("wrong formatted key")},
		{keyStr: "prefix/nb/db/table/id", prefix: "", expErr: fmt.Errorf("wrong key, unmatched prefix")},
		{keyStr: "ovsdb/nb/db/table/id", prefix: "ovsdb/nb", expKey: &Key{Prefix: "ovsdb/nb", DBName: "db", TableName: "table", UUID: "id"}},
	}
	for _, tcase := range tests {
		SetPrefix(tcase.prefix)
		key, err := ParseKey(tcase.keyStr)
		if tcase.expErr == nil {
			assert.Nilf(t, err, "[%s key] returned unexpected error %v", tcase.keyStr, err)
			assert.Equal(t, tcase.expKey, key, "[%s key] wrong key parsing", tcase.keyStr)
		} else {
			assert.ErrorContainsf(t, err, tcase.expErr.Error(), "[%s key] returned wrong error %v", tcase.keyStr, err)
		}
	}
}
