package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ibm/ovsdb-etcd/pkg/common"
)

const baseData string = "../../tests/data/"
const baseTransact string = baseData + "/transact/"

func TestTransactSelect(t *testing.T) {

	byteValue, err := common.ReadFile(baseTransact + "select-response.json")
	assert.Nil(t, err)
	expectedResponse := common.BytesToInterface(byteValue)
	var expectedError error
	db := &DatabaseMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	byteValue, err = common.ReadFile(baseTransact + "select-request.json")
	assert.Nil(t, err)
	requestArrayMapString := common.BytesToArrayMapString(byteValue)
	requestArrayInterface := common.ArrayMapStringToArrayInterface(*requestArrayMapString)
	ch := NewHandler(context.Background(), db)
	actualResponse, actualError := ch.Transact(nil, requestArrayInterface)
	assert.Equal(t, expectedError, actualError)
	actualResponseString, err := json.Marshal(actualResponse)
	assert.Nil(t, err)
	expectedResponseString, err := json.Marshal(expectedResponse)
	assert.Nil(t, err)
	// FIXME: need to normalize before comparison
	// assert.Equal(t, string(expectedResponseString), string(actualResponseString))
	fmt.Printf("expected: %s\n", string(expectedResponseString))
	fmt.Printf("actual  : %s\n", string(actualResponseString))
}
