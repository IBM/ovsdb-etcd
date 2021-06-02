package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

type updater struct {
	Columns map[string]bool
	Where   [][]string
	Select  libovsdb.MonitorSelect
	isV1    bool
	// the update unique key, used as a map key instead of the updater itself
	key string
}

type handlerKey struct {
	handler   *Handler
	jsonValueStr string
	remoteAddr string
}

type handlerMonitorData struct {
	notificationType ovsjson.UpdateNotificationType

	updaters         map[string][]string
	dataBaseName     string
	jsonValue        interface{}
}

// Map from a key which represents a table paths (prefix/dbname/table) to arrays of updaters
// OVSDB allows specifying an array of <dbMonitor-request> objects for a monitored table
type Key2Updaters map[common.Key][]updater

type dbMonitor struct {
	// etcd watcher channel
	watchChannel clientv3.WatchChan
	// cancel function to close the etcd watcher
	cancel context.CancelFunc

	mu sync.Mutex
	db Databaser
	// database name that the dbMonitor is watching
	dataBaseName string

	// Map from etcd paths (prefix/dbname/table) to arrays of updaters
	// We use it to link keys from etcd events to updaters. We use array of updaters, because OVSDB allows to specify
	// an array of <dbMonitor-request> objects for a monitored table
	key2Updaters Key2Updaters

	// Map from updater keys to arrays of handlers
	// The map helps to link from the updaters discovered by 'key2Updaters' to relevant clients (handlers)
	updater2handlers map[string][]handlerKey
}

func newMonitor(dbName string, db Databaser) *dbMonitor {
	m := dbMonitor{dataBaseName: dbName, db: db}
	m.key2Updaters = Key2Updaters{}
	m.updater2handlers = map[string][]handlerKey{}
	return &m
}

func (m *dbMonitor) addUpdaters(keyToUpdaters Key2Updaters, handler handlerKey) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, updaters := range keyToUpdaters {
		_, ok := m.key2Updaters[key]
		if !ok {
			m.key2Updaters[key] = []updater{}
		}

	Outer:
		for _, uNew := range updaters {
			if _, ok := m.updater2handlers[uNew.key]; !ok {
				m.updater2handlers[uNew.key] = []handlerKey{}
			}
			m.updater2handlers[uNew.key] = append(m.updater2handlers[uNew.key], handler)
			for _, u1 := range m.key2Updaters[key] {
				if uNew.key == u1.key {
					continue Outer
				}
			}
			m.key2Updaters[key] = append(m.key2Updaters[key], uNew)
		}
	}
}

func (m *dbMonitor) removeUpdaters(updaters map[string][]string, handler handlerKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for table, updaterkeys := range updaters {
		tableKey := common.NewTableKey(m.dataBaseName, table)
		for _, updaterKey := range updaterkeys {
			handlers := m.updater2handlers[updaterKey]
			for i, v := range handlers {
				if reflect.DeepEqual(v, handler) {
					m.updater2handlers[updaterKey] = append(handlers[:i], handlers[i+1:]...)
					break
				}
			}
			if len(m.updater2handlers[updaterKey]) == 0 {
				delete(m.updater2handlers, updaterKey)
				updt := m.key2Updaters[tableKey]
				for i, v := range updt {
					if v.key == updaterKey {
						m.key2Updaters[tableKey] = append(updt[:i], updt[i+1:]...)
						break
					}
				}
			}
		}
	}
	if !m.hasHandlers() {
		// there is no handlers, we can just destroy the entire dbMonitor object
		// clean the tables if the dbMonitor will not be destroyed
		m.key2Updaters = Key2Updaters{}
		m.updater2handlers = map[string][]handlerKey{}
		return
	}
	return
}

func (m *dbMonitor) start() {
	go func() {
		for wresp := range m.watchChannel {
			if wresp.Canceled {
				// TODO should we just reconnect
				m.mu.Lock()
				for _, handlersArray := range m.updater2handlers {
					for _, hlk := range handlersArray {
						// run in separate goroutines
						go hlk.handler.monitorCanceledNotification(hlk.jsonValueStr)
					}
				}
				m.mu.Unlock()
				// remove itself
				m.db.RemoveMonitor(m.dataBaseName)
				return
			}
			result, _ := m.prepareTableUpdate(wresp.Events)
			for hd, tu := range result {
				go hd.handler.notify(hd.jsonValueStr, tu)
			}
		}
	}()
}

func (m *dbMonitor) hasHandlers() bool {
	return len(m.updater2handlers) > 0
}

func mcrToUpdater(mcr ovsjson.MonitorCondRequest, isV1 bool) *updater {
	sort.Strings(mcr.Columns)
	var key string
	for _, c := range mcr.Columns {
		key = key + c
	}
	// TODO handle "Where"
	if mcr.Select == nil {
		mcr.Select = &libovsdb.MonitorSelect{}
	}
	key = fmt.Sprintf("%s%v%v", key, isV1, *mcr.Select)
	return &updater{Columns: common.StringArrayToMap(mcr.Columns), isV1: isV1, Select: *mcr.Select, key: key}
}

func (m *dbMonitor) prepareTableUpdate(events []*clientv3.Event) (map[handlerKey]ovsjson.TableUpdates, error) {
	klog.Infof("prepareTableUpdate: \n m.key2Updaters %v \n\n m.updater2handlers %v\n", m.key2Updaters, m.updater2handlers)

	// prepare results that will be sent to clients
	// handlerKey -> tableName -> uuid -> rowUpdate
	result := map[handlerKey]ovsjson.TableUpdates{}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ev := range events {
		key, err := common.ParseKey(string(ev.Kv.Key))
		if err != nil {
			klog.Errorf("Wrong event's key %s", string(ev.Kv.Key))
			continue
		}
		updaters, ok := m.key2Updaters[key.ToTableKey()]
		if !ok {
			klog.Infof("There is no monitors for table path %s", key.TableKeyString())
			continue
		}
		for _, updater := range updaters {
			rowUpdate, uuid, err := updater.prepareRowUpdate(ev)
			if err != nil {
				klog.Errorf("prepareRowUpdate returned error %s, updater %v", err, updater)
				continue
			}
			if rowUpdate == nil {
				// there is no updates
				continue
			}
			hKeys, ok := m.updater2handlers[updater.key]
			if !ok {
				klog.Errorf("Cannot find handlers for the updater %#v", updater)
			}
			for _, hKey := range hKeys {
				tableUpdates, ok := result[hKey]
				if !ok {
					tableUpdates = ovsjson.TableUpdates{} //map[string]map[string]ovsjson.RowUpdate{}
					result[hKey] = tableUpdates
				}
				tableUpdate, ok := tableUpdates[key.TableName]
				if !ok {
					tableUpdate = ovsjson.TableUpdate{} // map[string]ovsjson.RowUpdate{}
					tableUpdates[key.TableName] = tableUpdate
				}
				// check if there is a rowUpdate for the same uuid
				_, ok = tableUpdate[uuid]
				if ok {
					klog.Warningf("Duplicate event for %s\n prevRowUpdate %v \n newRowUpdate %v \n hKeys length = %d hKeys = %v hKey = %+v",
						key.ShortString(), tableUpdate[uuid], rowUpdate, len(hKeys), hKeys, hKey )
				}
				tableUpdate[uuid] = *rowUpdate
			}
		}
	}
	return result, nil
}

func (u *updater) prepareRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, string, error) {
	if !event.IsModify() { // the create or delete
		if event.IsCreate() {
			// Create event
			return u.prepareCreateRowUpdate(event)
		} else {
			// Delete event
			return u.prepareDeleteRowUpdate(event)
		}
	}
	// the event is modify
	return u.prepareModifyRowUpdate(event)
}

func (u *updater) prepareDeleteRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, string, error) {
	// Delete event
	if !libovsdb.MSIsTrue(u.Select.Delete) {
		return nil, "", nil
	}
	value := event.PrevKv.Value
	if !u.isV1 {
		// according to https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/#update2-notification,
		// "<row> is always a null object for a delete update."
		_, uuid, err := u.prepareRow(value)
		if err != nil {
			return nil, "", err
		}
		return &ovsjson.RowUpdate{Delete: true}, uuid, nil
	}

	data, uuid, err := u.prepareRow(value)
	if err != nil {
		return nil, "", err
	}
	if len(data) > 0 {
		// the delete for !u.isV1 we have returned before
		return &ovsjson.RowUpdate{Old: &data}, uuid, nil
	}
	return nil, uuid, nil
}

func (u *updater) prepareCreateRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, string, error) {
	// the event is create
	if !libovsdb.MSIsTrue(u.Select.Insert) {
		return nil, "", nil
	}
	value := event.Kv.Value
	data, uuid, err := u.prepareRow(value)
	if err != nil {
		return nil, "", err
	}
	if len(data) > 0 {
		if !u.isV1 {
			return &ovsjson.RowUpdate{Insert: &data}, uuid, nil
		}
		return &ovsjson.RowUpdate{New: &data}, uuid, nil
	}
	return nil, "", nil
}

func (u *updater) prepareModifyRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, string, error) {
	// the event is modify
	if !libovsdb.MSIsTrue(u.Select.Modify) {
		return nil, "", nil
	}
	data, uuid, err := u.prepareRow(event.Kv.Value)
	if err != nil {
		return nil, "", err
	}
	prevData, prevUUID, err := u.prepareRow(event.PrevKv.Value)
	if err != nil {
		return nil, "", err
	}
	if uuid != prevUUID {
		return nil, "", fmt.Errorf("UUID was changed prev uuid=%q, new uuid=%q", prevUUID, uuid)
	}
	for column, cValue := range data {
		// TODO use schema based comparison
		if reflect.DeepEqual(cValue, prevData[column]) {
			// TODO compare sets and maps
			if u.isV1 {
				delete(prevData, column)
			} else {
				delete(data, column)
			}
		}
	}
	if !u.isV1 {
		if len(data) > 0 {
			return &ovsjson.RowUpdate{Modify: &data}, uuid, nil
		}
	} else {
		if len(prevData) > 0 { // there are monitored updates
			return &ovsjson.RowUpdate{New: &data, Old: &prevData}, uuid, nil
		}
	}
	return nil, "", nil
}

func (u *updater) prepareCreateRowInitial(value *[]byte) (*ovsjson.RowUpdate, string, error) {
	if !libovsdb.MSIsTrue(u.Select.Initial) {
		return nil, "", nil
	}
	data, uuid, err := u.prepareRow(*value)
	if err != nil {
		return nil, "", err
	}
	if len(data) > 0 {
		if !u.isV1 {
			return &ovsjson.RowUpdate{Initial: &data}, uuid, nil
		}
		return &ovsjson.RowUpdate{New: &data}, uuid, nil
	}
	return nil, uuid, nil
}

func (u *updater) deleteUnselectedColumns(data map[string]interface{}) {
	if len(u.Columns) != 0 {
		for column := range data {
			if _, ok := u.Columns[column]; !ok {
				delete(data, column)
			}
		}
	}
}

func unmarshalData(data []byte) (map[string]interface{}, error) {
	obj := map[string]interface{}{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func getAndDeleteUUID(data map[string]interface{}) (string, error) {
	uuidInt, ok := data[COL_UUID]
	if !ok {
		return "", fmt.Errorf("row doesn't contain %s", COL_UUID)
	}
	delete(data, COL_UUID)
	uuid, ok := uuidInt.([]interface{})
	if !ok {
		return "", fmt.Errorf("wrong uuid type %T %v", uuidInt, uuidInt)
	}
	// TODO add uuid parsing
	if len(uuid) != 2 {
		return "", fmt.Errorf("wrong uuid type %v", uuid)
	}
	uuidStr, ok := uuid[1].(string)
	if !ok {
		return "", fmt.Errorf("wrong type %T %v", uuidInt, uuidInt)
	}
	return uuidStr, nil
}

func (u *updater) prepareRow(value []byte) (map[string]interface{}, string, error) {
	data, err := unmarshalData(value)
	if err != nil {
		return nil, "", err
	}
	uuid, err := getAndDeleteUUID(data)
	if err != nil {
		return nil, "", err
	}
	u.deleteUnselectedColumns(data)
	return data, uuid, nil
}
