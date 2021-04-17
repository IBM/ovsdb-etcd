package ovsdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

const baseMonitor string = "../../tests/data/monitor/"

func TestTableUpdate(t *testing.T) {
	tests := map[string]struct {
		updater      updater
		event        clientv3.Event
		expTable     string
		expUuid      string
		expRowUpdate *ovsjson.RowUpdate
		err          error
	}{"wrong key": {updater: updater{isV1: true},
		err:   fmt.Errorf("wrong formated key: %s", "key/a"),
		event: clientv3.Event{Kv: &mvccpb.KeyValue{Key: []byte("key/a"), Value: []byte("some value")}}},
		"allColumns-v1-Create": {updater: updater{isV1: true},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}"), CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: &ovsjson.RowUpdate{New: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
		"allColumns-v1-Delete": {updater: updater{isV1: true},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
			expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
		"allColumns-v1-Update": {updater: updater{isV1: true},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v3\"}"), CreateRevision: 1, ModRevision: 2}},
			expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2"}, New: &map[string]interface{}{"c1": "v1", "c2": "v3"}}},
		"SingleColumn-v1-Create": {updater: updater{isV1: true, Columns: map[string]bool{"c1": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}"), CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: &ovsjson.RowUpdate{New: &map[string]interface{}{"c1": "v1"}}},
		"ZeroColumn-v1-Create": {updater: updater{isV1: true, Columns: map[string]bool{"c3": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}"), CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: nil},
		"SingleColumn-v1-Delete": {updater: updater{isV1: true, Columns: map[string]bool{"c1": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
			expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c1": "v1"}}},
		"ZeroColumn-v1-Delete": {updater: updater{isV1: true, Columns: map[string]bool{"c3": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
			expRowUpdate: nil},
		"SingleColumn-v1-Update": {updater: updater{isV1: true, Columns: map[string]bool{"c2": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v3\"}"), CreateRevision: 1, ModRevision: 2}},
			expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2"}, New: &map[string]interface{}{"c2": "v3"}}},
		"ZeroColumn-v1-Update": {updater: updater{isV1: true, Columns: map[string]bool{"c1": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v3\"}"), CreateRevision: 1, ModRevision: 2}},
			expRowUpdate: nil},

		"allColumns-v2-Create": {updater: updater{isV1: false},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}"), CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: &ovsjson.RowUpdate{Insert: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
		"allColumns-v2-Delete": {updater: updater{isV1: false},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
			expRowUpdate: &ovsjson.RowUpdate{Delete: nil}},
		"allColumns-v2-Update": {updater: updater{isV1: false},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v3\"}"), CreateRevision: 1, ModRevision: 2}},
			expRowUpdate: &ovsjson.RowUpdate{Modify: &map[string]interface{}{"c2": "v3"}}},
		"SingleColumn-v2-Create": {updater: updater{isV1: false, Columns: map[string]bool{"c1": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}"), CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: &ovsjson.RowUpdate{Insert: &map[string]interface{}{"c1": "v1"}}},
		"ZeroColumn-v2-Create": {updater: updater{isV1: false, Columns: map[string]bool{"c3": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}"), CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: nil},
		"SingleColumn-v2-Delete": {updater: updater{isV1: false, Columns: map[string]bool{"c1": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
			expRowUpdate: &ovsjson.RowUpdate{Delete: nil}},
		"ZeroColumn-v2-Delete": {updater: updater{isV1: false, Columns: map[string]bool{"c3": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
			expRowUpdate: &ovsjson.RowUpdate{Delete: nil}},
		"SingleColumn-v2-Update": {updater: updater{isV1: false, Columns: map[string]bool{"c2": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v3\"}"), CreateRevision: 1, ModRevision: 2}},
			expRowUpdate: &ovsjson.RowUpdate{Modify: &map[string]interface{}{"c2": "v3"}}},
		"ZeroColumn-v2-Update": {updater: updater{isV1: false, Columns: map[string]bool{"c1": true}},
			expTable: "table", expUuid: "uuid",
			event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v2\"}")},
				Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"), Value: []byte("{\"c1\":\"v1\",\"c2\":\"v3\"}"), CreateRevision: 1, ModRevision: 2}},
			expRowUpdate: nil},
	}
	for name, ts := range tests {
		tableName, uuid, row, err := ts.updater.prepareTableUpdate(&ts.event)
		if ts.err != nil {
			assert.EqualErrorf(t, err, ts.err.Error(), "[%s test] expected error %s, got %v", name, ts.err.Error(), err)
			continue
		} else {
			assert.Nilf(t, err, "[%s test] returned unexpected error %v", name, err)
		}
		assert.Equalf(t, ts.expTable, tableName, "[%s test] returned wrong table name, expected %q, got %q", name, ts.expTable, tableName)
		assert.Equalf(t, ts.expUuid, uuid, "[%s test] returned wrong row uuid, expected %q, got %q", name, ts.expUuid, uuid)
		if ts.expRowUpdate == nil {
			assert.Nilf(t, row, "[%s test] returned unexpected row %#v", name, row)
		} else {
			assert.NotNil(t, row, "[%s test] returned nil row", name)
			if ts.updater.isV1 {
				ok, msg := row.ValidateRowUpdate()
				assert.Truef(t, ok, "[%s test]  Row update is not valid %s %#v", name, msg, row)
			} else {
				ok, msg := row.ValidateRowUpdate2()
				assert.Truef(t, ok, "[%s test]  Row update is not valid %s %#v", name, msg, row)
			}

			assert.EqualValuesf(t, ts.expRowUpdate, row, "[%s test] returned wrong row update, expected %#v, got %#v", name, *ts.expRowUpdate, *row)
		}

	}

}

func TestMonitorResponse(t *testing.T) {
	/*
		dataBase := "OVN_Northbound"
		tableName := "Logical_Router"
		allColumns := []string{"enabled", "external_ids", "load_balancer", "name", "nat", "options", "policies", "ports", "static_routes"}
		columns := [][]string{{"enabled", "external_ids", "load_balancer", "name", "nat", "options", "policies", "ports", "static_routes", "_version"},
			{"name"}, {}}
		selects := []*libovsdb.MonitorSelect{nil, {Initial: true}, {Initial: false}}

		byteValue, err := common.ReadFile(baseMonitor + "monitor-data.json")
		assert.Nil(t, err)
		values := common.BytesToMapInterface(byteValue)
		expectedResponse, err := common.MapToEtcdResponse(values)
		var expectedError error
		dbServer := &DatabaseMock{
			Response: expectedResponse,
			Error:    expectedError,
		}
		ch := NewHandler(context.Background(), dbServer)
		cmp := ovsjson.CondMonitorParameters{}
		cmp.DatabaseName = dataBase
		cmp.JsonValue = nil
		mcr := ovsjson.MonitorCondRequest{}
		for _, sel := range selects {
			for _, col := range columns {
				mcr.Columns = col
				mcr.Select = sel
				cmp.MonitorCondRequests = map[string][]ovsjson.MonitorCondRequest{tableName: {mcr}}

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
		} */
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
