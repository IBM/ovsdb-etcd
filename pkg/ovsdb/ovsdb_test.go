package ovsdb

import (
	"encoding/json"
	"fmt"
	ovsjson "github.com/ibm/ovsdb-etcd/pkg/json"
	"testing"

	"github.com/ibm/ovsdb-etcd/pkg/common"

	"github.com/stretchr/testify/assert"
)

const BaseData string = "../../tests/data/"
const BaseTransact string = BaseData + "/transact/"
const BaseMonitor string = BaseData + "/monitor/"

func TestTransactSelect(t *testing.T) {

	byteValue, err := common.ReadFile(BaseTransact + "select-response.json")
	assert.Nil(t, err)
	expectedResponse := common.BytesToInterface(byteValue)
	var expectedError error
	dbServer := &DBServerMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	byteValue, err = common.ReadFile(BaseTransact + "select-request.json")
	assert.Nil(t, err)
	requestArrayMapString := common.BytesToArrayMapString(byteValue)
	requestArrayInterface := common.ArrayMapStringToArrayInterface(*requestArrayMapString)
	ch := NewClientHandler(dbServer)
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

func TestMonitorResponse(t *testing.T) {
	byteValue, err := common.ReadFile(BaseMonitor + "monitor-data.json")
	assert.Nil(t, err)
	values := common.BytesToArrayMapInterface(byteValue)
	expectedResponse, err := common.MapToEtcdResponse(values)
	var expectedError error
	dbServer := &DBServerMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	ch := NewClientHandler(dbServer)
	cmp := ovsjson.CondMonitorParameters{}
	cmp.DatabaseName = "OVN_Northbound"
	cmp.JsonValue = nil
	mcr := ovsjson.MonitorCondRequest{}
	mcr.Columns = []interface{}{"enabled","external_ids","load_balancer","name","nat","options","policies","ports","static_routes","_version"}
	cmp.MonitorCondRequests =  map[string][]ovsjson.MonitorCondRequest{"Logical_Router":[]ovsjson.MonitorCondRequest{mcr}}
	result, err := ch.Monitor(nil, cmp)
	assert.Nil(t, err)
	fmt.Printf("Result %v\n", result)

}