package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewServOVSDBMock() *ServOVSDB {
	dbserver, _ := NewDBServerMock()
	return &ServOVSDB{dbserver}
}

func TestOVSDBTransact(t *testing.T) {
	expected := "{Transact}"
	srv := NewServOVSDBMock()
	actual, err := srv.Transact(nil, []interface{}{})
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}
