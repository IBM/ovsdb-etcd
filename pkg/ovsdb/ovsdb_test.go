package ovsdb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const baseTransact string = "../../tests/data/transact/"

func TestTransactSelect(t *testing.T) {
	byteValue := readJson(t, baseTransact+"select-response.json")
	expectedResponse := bytesToInterface(byteValue)
	var expectedError error
	dbServer := &DBServerMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	byteValue = readJson(t, baseTransact+"select-request.json")
	requestArrayMapString := bytesToArrayMapString(byteValue)
	requestArrayInterface := arrayMapStringToArrayInterface(*requestArrayMapString)
	servOVSDB := &ServOVSDB{dbServer}
	actualResponse, actualError := servOVSDB.Transact(nil, requestArrayInterface)
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
