package ovsdb

import (
	"testing"

	"github.com/ebay/libovsdb"
	"github.com/stretchr/testify/assert"
)

func TestOperationSelect(t *testing.T) {
	expectedResponse := &[]map[string]string{}
	var expectedError error
	mock := &DBServerMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	doOp := &doOperation{
		dbServer: mock,
	}
	op := &libovsdb.Operation{}
	op.Op = "select"
	_, err := doOp.Select(op)
	assert.Equal(t, expectedError, err)
}
