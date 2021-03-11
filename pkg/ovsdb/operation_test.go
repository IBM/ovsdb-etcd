package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const base = "../../tests/data/operation/"

func TestOperationSelect(t *testing.T) {
	byteValue := readJson(t, base+"select-response.json")
	expectedResponse := toArrayMapString(byteValue)
	var expectedError error
	mock := &DBServerMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	doOp := &doOperation{
		dbServer: mock,
	}
	byteValue = readJson(t, base+"select-request.json")
	op := toOperation(byteValue)
	_, err := doOp.Select(op)
	assert.Equal(t, expectedError, err)
}
