package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"reflect"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

type updater struct {
	Columns map[string]bool
	Where   [][]string
	isV1    bool
}

type handlerKey struct {
	handler    *Handler
	jasonValue interface{}
}

type monitor struct {
	cancel context.CancelFunc
	mu     sync.Mutex

	updaters []updater
	insert   map[*updater][]*handlerKey
	delete   map[*updater][]*handlerKey
	modify   map[*updater][]*handlerKey
}

func mcrToUpdater(mcr ovsjson.MonitorCondRequest, isV1 bool) *updater {
	// TODO handle Where
	return &updater{Columns: common.StringArrayToMap(mcr.Columns), isV1: isV1}
}
func (m *monitor) addHandler(mcr ovsjson.MonitorCondRequest, isV1 bool, handler *handlerKey) {
	updater := mcrToUpdater(mcr, isV1)
	indx := -1
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, u := range m.updaters {
		if reflect.DeepEqual(updater, u) {
			indx = i
		}
	}
	if indx == -1 {
		// new entry
		indx = len(m.updaters)
		m.updaters = append(m.updaters, *updater)
	}
	if mcr.Select == nil || libovsdb.MSIsTrue(mcr.Select.Insert) {
		hand, ok := m.insert[&m.updaters[indx]]
		if ok {
			hand = append(hand, handler)
		} else {
			hand = []*handlerKey{handler}
		}
		m.insert[&m.updaters[indx]] = hand
	}
	if mcr.Select == nil || libovsdb.MSIsTrue(mcr.Select.Delete) {
		hand, ok := m.delete[&m.updaters[indx]]
		if ok {
			hand = append(hand, handler)
		} else {
			hand = []*handlerKey{handler}
		}
		m.delete[&m.updaters[indx]] = hand
	}
	if mcr.Select == nil || libovsdb.MSIsTrue(mcr.Select.Modify) {
		hand, ok := m.modify[&m.updaters[indx]]
		if ok {
			hand = append(hand, handler)
		} else {
			hand = []*handlerKey{handler}
		}
		m.modify[&m.updaters[indx]] = hand
	}
}

func (m *monitor) delHandler(mcr ovsjson.MonitorCondRequest, isV1 bool, handler *handlerKey) bool {
	updater := mcrToUpdater(mcr, isV1)
	indx := -1
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, u := range m.updaters {
		if reflect.DeepEqual(updater, u) {
			indx = i
		}
	}
	if indx == -1 {
		klog.Warningf("remove unexsisting handler %v", mcr)
	}
	if mcr.Select == nil || libovsdb.MSIsTrue(mcr.Select.Insert) {
		handlers := m.insert[&m.updaters[indx]]
		for k, v := range handlers {
			if v == handler {
				handlers[k] = handlers[len(handlers)-1]
				m.insert[&m.updaters[indx]] = handlers[:len(handlers)-1]
			}
		}
	}
	if mcr.Select == nil || libovsdb.MSIsTrue(mcr.Select.Delete) {
		handlers := m.delete[&m.updaters[indx]]
		for k, v := range handlers {
			if v == handler {
				handlers[k] = handlers[len(handlers)-1]
				m.delete[&m.updaters[indx]] = handlers[:len(handlers)-1]
			}
		}
	}
	if mcr.Select == nil || libovsdb.MSIsTrue(mcr.Select.Modify) {
		handlers := m.modify[&m.updaters[indx]]
		for k, v := range handlers {
			if v == handler {
				handlers[k] = handlers[len(handlers)-1]
				m.modify[&m.updaters[indx]] = handlers[:len(handlers)-1]
			}
		}
	}
	if len(m.modify[&m.updaters[indx]]) == 0 &&
		len(m.delete[&m.updaters[indx]]) == 0 &&
		len(m.insert[&m.updaters[indx]]) == 0 {
		// we can remove this handler
		m.updaters[indx] = m.updaters[len(m.updaters)-1]
		m.updaters = m.updaters[:len(m.updaters)-1]
		if len(m.updaters) == 0 {
			m.cancel()
			return true
		}
	}
	return false
}

func newMonitor(cli *clientv3.Client, prefix string, mcr ovsjson.MonitorCondRequest, isV1 bool, handler *handlerKey) *monitor {

	ctx := context.Background()
	ctxt, cancel := context.WithCancel(ctx)
	wch := cli.Watch(clientv3.WithRequireLeader(ctxt), prefix,
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV())

	monitor := &monitor{cancel: cancel}
	go func() {
		for wresp := range wch {
			for _, ev := range wresp.Events {
				var dest map[*updater][]*handlerKey
				monitor.mu.Lock()
				if ev.IsCreate() {
					// insert event
					dest = monitor.insert
				} else if ev.IsModify() {
					dest = monitor.modify
				} else {
					// delete event
					dest = monitor.delete
				}
				monitor.mu.Unlock()
				// TODO queue or a separate goroutine
				monitor.propagateEvent(ev, dest)
			}
		}
	}()
	return monitor
}

func (m *monitor) propagateEvent(event *clientv3.Event, destinations map[*updater][]*handlerKey) {
	// for each updater entry create a return data and call each handler
}

func (u *updater) prepareTableUpdate(event *clientv3.Event) (table string, uuid string, rowUpdate *ovsjson.RowUpdate, err error) {
	key := string(event.Kv.Key)
	if table, uuid, err = keyToTableAndUUID(key); err != nil {
		return
	}
	data := map[string]interface{}{}
	var value []byte
	if !event.IsModify() { // the create or delete
		if event.IsCreate() {
			value = event.Kv.Value
		} else {
			if !u.isV1 {
				rowUpdate = &ovsjson.RowUpdate{Delete: nil}
				return
			}
			value = event.PrevKv.Value
		}
		if err = json.Unmarshal(value, &data); err != nil {
			return
		}
		delete(data, "uuid")
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
					rowUpdate = &ovsjson.RowUpdate{Insert: &data}
				} else {
					rowUpdate = &ovsjson.RowUpdate{New: &data}
				}
			} else {
				// the delete for !u.isV1 we have returned before
				rowUpdate = &ovsjson.RowUpdate{Old: &data}
			}
		}
	} else { // the event is modify
		value = event.Kv.Value
		data := map[string]interface{}{}
		if err = json.Unmarshal(value, &data); err != nil {
			return
		}
		prevValue := event.PrevKv.Value
		prevData := map[string]interface{}{}
		if err = json.Unmarshal(prevValue, &prevData); err != nil {
			return
		}
		delete(data, "uuid")
		delete(prevData, "uuid")
		for column, cValue := range data {
			if len(u.Columns) != 0 {
				if _, ok := u.Columns[column]; !ok {
					delete(data, column)
					delete(prevData, column)
					continue
				}
			}
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
				rowUpdate = &ovsjson.RowUpdate{Modify: &data}
			}
		} else {
			if len(prevData) > 0 { // there are monitored updates
				rowUpdate = &ovsjson.RowUpdate{New: &data, Old: &prevData}
			}
		}
	}
	return
}

func keyToTableAndUUID(key string) (table string, uuid string, err error) {
	slices := strings.Split(key, "/")
	l := len(slices)
	if l < 3 {
		return "", "", fmt.Errorf("wrong formated key: %s", key)
	}
	return slices[l-2], slices[l-1], nil
}
