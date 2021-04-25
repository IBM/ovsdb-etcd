package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog"

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
	jsonValue interface{}
}

type handlerMonitorData struct {
	notificationType ovsjson.UpdateNotificationType
	updaters         map[string][]string
	dataBaseName     string
}

type monitor struct {
	// etcd watcher channel
	watchChannel clientv3.WatchChan
	// cancel function to close the etcd watcher
	cancel context.CancelFunc

	mu sync.Mutex
	db Databaser
	// database name that the monitor is watching
	dataBaseName string

	// Map from etcd paths (prefix/dbname/table) to arrays of updaters
	// We use it to link keys from etcd events to updaters. We use array of updaters, because OVSDB allows to specify
	// an array of <monitor-request> objects for a monitored table
	key2Updaters map[string][]updater

	// Map from updater keys to arrays of handlers
	// The map helps to link from the updaters discovered by 'key2Updaters' to relevant clients (handlers)
	upater2handlers map[string][]handlerKey

	// all handlers
	handlers map[handlerKey]bool
}

var allTrueSelect = libovsdb.MonitorSelect{Initial: true, Insert: true, Delete: true, Modify: true}

func newMonitor(dbName string, db Databaser) *monitor {
	m := monitor{dataBaseName: dbName, db: db}
	m.key2Updaters = map[string][]updater{}
	m.upater2handlers = map[string][]handlerKey{}
	m.handlers = map[handlerKey]bool{}
	return &m
}

func (m *monitor) addUpdaters(dbPrefix string, updaters map[string][]*updater, handler handlerKey) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for table, updaters := range updaters {
		table = m.tableToKey(dbPrefix, table)
		_, ok := m.key2Updaters[table]
		if !ok {
			m.key2Updaters[table] = []updater{}
		}

	Outer:
		for _, uNew := range updaters {
			if _, ok := m.upater2handlers[uNew.key]; !ok {
				m.upater2handlers[uNew.key] = []handlerKey{}
			}
			m.upater2handlers[uNew.key] = append(m.upater2handlers[uNew.key], handler)
			for _, u1 := range m.key2Updaters[table] {
				if uNew.key == u1.key {
					continue Outer
				}
			}
			m.key2Updaters[table] = append(m.key2Updaters[table], *uNew)
		}
	}
	m.handlers[handler] = true
}

func (m *monitor) removeUpdaters(dbPrefix string, updaters map[string][]string, handler handlerKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ok := m.handlers[handler]; !ok {
		klog.Warningf("Removing nonexistent handler %v", handler)
		return
	}
	delete(m.handlers, handler)
	if len(m.handlers) == 0 {
		// there is no handlers, we can just destroy the entire monitor object
		// clean the tables if the monitor will not be destroyed
		m.key2Updaters = map[string][]updater{}
		m.upater2handlers = map[string][]handlerKey{}
		return
	}
	for table, updaterkeys := range updaters {
		table = m.tableToKey(dbPrefix, table)
		for _, updaterKey := range updaterkeys {
			handlers := m.upater2handlers[updaterKey]
			for i, v := range handlers {
				if v == handler {
					m.upater2handlers[updaterKey] = append(handlers[:i], handlers[i+1:]...)
					break
				}
			}
			if len(m.upater2handlers[updaterKey]) == 0 {
				delete(m.upater2handlers, updaterKey)
				updt := m.key2Updaters[table]
				for i, v := range updt {
					if v.key == updaterKey {
						m.key2Updaters[table] = append(updt[:i], updt[i+1:]...)
						break
					}
				}
			}
		}
	}
	return
}

func (m *monitor) start() {
	go func() {
		for wresp := range m.watchChannel {
			if wresp.Canceled {
				// TODO should we just reconnect
				m.mu.Lock()
				for hlk := range m.handlers {
					// run in separate goroutines
					go hlk.handler.monitorCanceledNotification(hlk.jsonValue)
				}
				m.mu.Unlock()
				// remove itself
				m.db.RemoveMonitor(m.dataBaseName)
				return
			}
			result, _ := m.prepareTableUpdate(wresp.Events)
			for hd, tu := range result {
				go hd.handler.notify(hd.jsonValue, tu)
			}
		}
	}()
}

func (m *monitor) hasHandlers() bool {
	return len(m.handlers) > 0
}

func mcrToUpdater(mcr ovsjson.MonitorCondRequest, isV1 bool) *updater {
	sort.Strings(mcr.Columns)
	var key string
	for _, c := range mcr.Columns {
		key = key + c
	}
	// TODO handle "Where"
	if mcr.Select == nil {
		mcr.Select = &allTrueSelect
	}
	key = fmt.Sprintf("%s%v%v", key, isV1, *mcr.Select)
	return &updater{Columns: common.StringArrayToMap(mcr.Columns), isV1: isV1, Select: *mcr.Select, key: key}
}

func (m *monitor) prepareTableUpdate(events []*clientv3.Event) (map[handlerKey]ovsjson.TableUpdates, error) {
	// prepare results that will be sent to clients
	// handlerKey -> tableName -> uuid -> rowUpdate
	result := map[handlerKey]ovsjson.TableUpdates{}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ev := range events {
		key := string(ev.Kv.Key)
		path, table, uuid, err := keyToTableAndUUID(key)
		if err != nil {
			klog.Errorf("Wrong event's key %s", key)
			continue
		}
		updaters, ok := m.key2Updaters[path]
		if !ok {
			klog.Infof("There is no monitors for table path %s", path)
			continue
		}
		for _, updater := range updaters {
			rowUpdate, err := updater.prepareRowUpdate(ev)
			if err != nil {
				klog.Errorf("prepareRowUpdate returned error %s, updater %v", err, updater)
				continue
			}
			if rowUpdate == nil {
				// there is no updates
				continue
			}
			hKeys, ok := m.upater2handlers[updater.key]
			if !ok {
				klog.Errorf("Cannot find handlers for the updater %#v", updater)
			}
			for _, hKey := range hKeys {
				tableUpdates, ok := result[hKey]
				if !ok {
					tableUpdates = ovsjson.TableUpdates{} //map[string]map[string]ovsjson.RowUpdate{}
					result[hKey] = tableUpdates
				}
				tableUpdate, ok := tableUpdates[table]
				if !ok {
					tableUpdate = ovsjson.TableUpdate{} // map[string]ovsjson.RowUpdate{}
					tableUpdates[table] = tableUpdate
				}
				// check if there is a rowUpdate for the same uuid
				_, ok = tableUpdate[uuid]
				if ok {
					// TODO key unification
					klog.Warningf("Duplicate event for %s/%s/%s", m.dataBaseName, table, uuid)
				}
				tableUpdate[uuid] = *rowUpdate
			}
		}
	}
	return result, nil
}

func (u *updater) prepareRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, error) {
	var value []byte
	if !event.IsModify() { // the create or delete
		if event.IsCreate() {
			// Create event
			if !u.Select.Insert {
				return nil, nil
			}
			value = event.Kv.Value
		} else {
			// Delete event
			if !u.Select.Delete {
				return nil, nil
			}
			if !u.isV1 {
				// according to https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/#update2-notification,
				// "<row> is always a null object for a delete update."
				return &ovsjson.RowUpdate{Delete: nil}, nil
			}
			value = event.PrevKv.Value
		}
		data, err := unmarshalData(value)
		if err != nil {
			return nil, err
		}
		if len(u.Columns) != 0 {
			for column := range data {
				if _, ok := u.Columns[column]; !ok {
					delete(data, column)
				}
			}
		}
		if len(data) > 0 {
			if event.IsCreate() {
				if !u.isV1 {
					return &ovsjson.RowUpdate{Insert: &data}, nil
				}
				return &ovsjson.RowUpdate{New: &data}, nil
			}
			// the delete for !u.isV1 we have returned before
			return &ovsjson.RowUpdate{Old: &data}, nil
		}
		return nil, nil
	}
	// the event is modify
	if !u.Select.Modify {
		return nil, nil
	}
	data, err := unmarshalData(event.Kv.Value)
	if err != nil {
		return nil, err
	}
	prevData, err := unmarshalData(event.PrevKv.Value)
	if err != nil {
		return nil, err
	}
	for column, cValue := range data {
		if len(u.Columns) != 0 {
			if _, ok := u.Columns[column]; !ok {
				delete(data, column)
				delete(prevData, column)
				continue
			}
		}
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
			return &ovsjson.RowUpdate{Modify: &data}, nil
		}
	} else {
		if len(prevData) > 0 { // there are monitored updates
			return &ovsjson.RowUpdate{New: &data, Old: &prevData}, nil
		}
	}
	return nil, nil
}

func unmarshalData(data []byte) (map[string]interface{}, error) {
	obj := map[string]interface{}{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}
	// TODO use a constant
	delete(obj, "_uuid")
	return obj, nil
}

func (m *monitor) tableToKey(dbPrefix string, table string) string {
	// TODO key unification
	return fmt.Sprintf("%s%s/%s", dbPrefix, m.dataBaseName, table)
}

func keyToTableAndUUID(key string) (path string, table string, uuid string, err error) {
	// TODO key unification
	slices := strings.Split(key, "/")
	l := len(slices)
	if l < 4 {
		err = fmt.Errorf("wrong formated key: %s", key)
		return
	}
	path = fmt.Sprintf("%s/%s/%s", slices[0], slices[1], slices[2])
	table = slices[l-2]
	uuid = slices[l-1]
	return
}
