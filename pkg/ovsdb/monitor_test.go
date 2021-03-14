package ovsdb

import (
	"fmt"
	"github.com/ebay/libovsdb"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

const baseMonitor string = baseData + "/monitor/"

func TestMonitorResponse(t *testing.T) {
	dataBase := "OVN_Northbound"
	tableName := "Logical_Router"
	allColumns := []string{"enabled", "external_ids", "load_balancer", "name", "nat", "options", "policies", "ports", "static_routes"}
	columns := [][]string{{"enabled", "external_ids", "load_balancer", "name", "nat", "options", "policies", "ports", "static_routes", "_version"},
		{"name"}, {}}
	selects := []*libovsdb.MonitorSelect{nil, &libovsdb.MonitorSelect{Initial: true}, &libovsdb.MonitorSelect{Initial: false}}

	byteValue, err := common.ReadFile(baseMonitor + "monitor-data.json")
	assert.Nil(t, err)
	values := common.BytesToMapInterface(byteValue)
	expectedResponse, err := common.MapToEtcdResponse(values)
	var expectedError error
	dbServer := &DatabaseMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	ch := NewHandler(dbServer)
	cmp := ovsjson.CondMonitorParameters{}
	cmp.DatabaseName = dataBase
	cmp.JsonValue = nil
	mcr := ovsjson.MonitorCondRequest{}
	for _, sel := range selects {
		for _, col := range columns {
			mcr.Columns = col
			mcr.Select = sel
			cmp.MonitorCondRequests = map[string][]ovsjson.MonitorCondRequest{tableName: []ovsjson.MonitorCondRequest{mcr}}

			monitorResponse, actualError := ch.Monitor(nil, cmp)
			assert.Nilf(t, actualError, "handler call should not return error: %v", err)

			monitorCondResponse, actualError := ch.MonitorCond(nil, cmp)
			assert.Nilf(t, actualError, "handler call should not return error: %v", err)

			monitorSinceResponse, actualError := ch.MonitorCondSince(nil, cmp)
			assert.Nilf(t, actualError, "handler call should not return error: %v", err)

			testResponse := func(resp ovsjson.TableUpdates, isV1 bool) {
				if sel != nil && !sel.Initial {
					assert.Truef(t, len(resp) == 0, "If Select.Initial is `false` monitor requests should not return data")
					return
				}
				lr, ok := resp[tableName]
				assert.True(t, ok)
				assert.True(t, len(lr) == len(*values))
				for name := range *values {
					lr1, ok := lr[name]
					assert.True(t, ok)
					if len(col) == 0 {
						col = allColumns
					}
					if isV1 {
						val, msg := lr1.ValidateRowUpdate()
						assert.Truef(t, val, msg)
						assert.NotNilf(t, lr1.New, "the response should include `new` entries")
						assert.Truef(t, len(*lr1.New) > 0, "the response should include several `new` entries")
						validateColumns(t, col, *lr1.New)
					} else {
						val, msg := lr1.ValidateRowUpdate2()
						assert.Truef(t, val, msg)
						assert.NotNilf(t, lr1.Initial, "the response should include 'initial` entries")
						assert.Truef(t, len(*lr1.Initial) > 0, "the response should include several `initial` entries")
						validateColumns(t, col, *lr1.Initial)
					}
				}
			}
			testResponse(monitorResponse.(ovsjson.TableUpdates), true)
			testResponse(monitorCondResponse.(ovsjson.TableUpdates), false)
			repsArray := monitorSinceResponse.([]interface{})
			assert.Truef(t, len(repsArray) == 3, "MonitorCondSince response should contains 3 elements")
			found, ok := repsArray[0].(bool)
			assert.Truef(t, ok, "found should be bool")
			assert.Falsef(t, found, "we don't support since transactions")
			tableUpdate, ok := repsArray[2].(ovsjson.TableUpdates)
			assert.Truef(t, ok, "the last response element should be table-updates2")
			testResponse(tableUpdate, false)
		}
	}
}

func validateColumns(t *testing.T, requireColumns []string, actualData map[string]interface{}) {
	assert.Equal(t, len(requireColumns), len(actualData), "they should be same length\n"+
		fmt.Sprintf("expected: %v\n", requireColumns)+
		fmt.Sprintf("actual  : %v\n", actualData))
	for _, column := range requireColumns {
		_, ok := actualData[column]
		assert.Truef(t, ok, "actual data should include %s columns", column)
	}
}
