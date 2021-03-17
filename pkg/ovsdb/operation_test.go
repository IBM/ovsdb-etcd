package ovsdb

import (
	"testing"

	"github.com/ibm/ovsdb-etcd/pkg/common"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const base = "../../tests/data/operation/"

func TestOperationSelect(t *testing.T) {
	// byteValue, err := common.ReadFile(base + "select-response.json")
	// assert.Nil(t, err)
	// expectedResponse := common.BytesToArrayMapString(byteValue)
	expectedResponse := &clientv3.GetResponse{}
	var expectedError error
	db := &DatabaseMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	doOp := &doOperation{
		db: db,
	}
	byteValue, err := common.ReadFile(base + "select-request.json")
	assert.Nil(t, err)
	op := common.BytesToOperation(byteValue)
	_, err = doOp.Select(op)
	assert.Equal(t, expectedError, err)
}
