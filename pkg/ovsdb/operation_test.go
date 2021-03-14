package ovsdb

import (
	"testing"

	"github.com/ibm/ovsdb-etcd/pkg/common"

	"github.com/stretchr/testify/assert"
)

const base = "../../tests/data/operation/"

func TestOperationSelect(t *testing.T) {
	byteValue, err := common.ReadFile(base + "select-response.json")
	assert.Nil(t, err)
	expectedResponse := common.BytesToArrayMapString(byteValue)
	var expectedError error
	mock := &DBServerMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	doOp := &doOperation{
		dbServer: mock,
	}
	byteValue, err = common.ReadFile(base + "select-request.json")
	assert.Nil(t, err)
	op := common.BytesToOperation(byteValue)
	_, err = doOp.Select(op)
	assert.Equal(t, expectedError, err)
}
