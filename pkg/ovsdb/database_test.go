package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMockLock(t *testing.T) {
	expectedResponse := true
	var expectedError error
	mock := DatabaseMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	actualResponse, actualError := mock.Lock(nil, "")
	assert.Equal(t, expectedResponse, actualResponse)
	assert.Equal(t, expectedError, actualError)
}

func TestMockUnlock(t *testing.T) {
	var expectedError error
	mock := DatabaseMock{
		Error: expectedError,
	}
	actualError := mock.Unlock(nil, "")
	assert.Equal(t, expectedError, actualError)
}

func TestMockAddSchema(t *testing.T) {
	var expectedError error
	mock := DatabaseMock{
		Error: expectedError,
	}
	actualError := mock.AddSchema("", "")
	assert.Equal(t, expectedError, actualError)
}

func TestMockGetData(t *testing.T) {
	var expectedResponse *clientv3.GetResponse
	var expectedError error
	mock := DatabaseMock{
		Error:    expectedError,
		Response: expectedResponse,
	}
	actualResponse, actualError := mock.GetData("", true)
	assert.Equal(t, expectedError, actualError)
	assert.Equal(t, expectedResponse, actualResponse)
}

func TestMockGetMarshaled(t *testing.T) {
	expectedResponse := &[]map[string]string{
		{
			"key1": "val1",
			"key2": "val2",
		},
	}
	var expectedError error
	mock := DatabaseMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	actualResponse, actualError := mock.GetMarshaled("", []interface{}{})
	assert.Equal(t, expectedError, actualError)
	assert.Equal(t, expectedResponse, actualResponse)
}

func TestMockGetSchema(t *testing.T) {
	expectedResponse := "val1"
	expectedOk := true
	mock := DatabaseMock{
		Response: expectedResponse,
		Ok:       expectedOk,
	}
	actualResponse, actualOk := mock.GetSchema("")
	assert.Equal(t, expectedOk, actualOk)
	assert.Equal(t, expectedResponse, actualResponse)
}

func TestMockGetUUID(t *testing.T) {
	expectedResponse := "val1"
	mock := DatabaseMock{
		Response: expectedResponse,
	}
	actualResponse := mock.GetUUID()
	assert.Equal(t, expectedResponse, actualResponse)
}
