package ovsdb

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

const (
	MonitorCanceled = "monitor_canceled"
	Update          = "update"
	Update2         = "update2"
	Update3         = "update3"
)

type updater struct {
	mcr              ovsjson.MonitorCondRequest
	tableSchema      *libovsdb.TableSchema
	isV1             bool
	notificationType ovsjson.UpdateNotificationType
	jasonValueStr    string
	log              logr.Logger
}

type handlerMonitorData struct {
	log logr.Logger

	notificationType ovsjson.UpdateNotificationType

	// updaters from the given json-value, key is the path in the monitor.
	updatersKeys      []common.Key
	dataBaseName      string
	jsonValue         interface{}
	notificationChain chan notificationEvent
}

type notificationEvent struct {
	updates  ovsjson.TableUpdates
	revision int64
}

// Map from a key which represents a table paths (prefix/dbname/table) to arrays of updaters
// OVSDB allows specifying an array of <dbMonitor-request> objects for a monitored table
type Key2Updaters map[common.Key][]updater

func (k *Key2Updaters) removeUpdaters(key common.Key, jsonValue string) {
	updaters, ok := (*k)[key]
	if !ok {
		return
	}
	var newUpdaters []updater
	for _, u := range updaters {
		if u.jasonValueStr != jsonValue {
			newUpdaters = append(newUpdaters, u)
		}
	}
	if len(newUpdaters) != 0 {
		(*k)[key] = newUpdaters
	} else {
		delete(*k, key)
	}
}

type dbMonitor struct {
	log logr.Logger

	// etcdTrx watcher channel
	watchChannel clientv3.WatchChan
	// cancel function to close the etcdTrx watcher
	cancel context.CancelFunc

	mu sync.Mutex
	// database name that the dbMonitor is watching
	dataBaseName string

	// Map from etcdTrx paths (prefix/dbname/table) to arrays of updaters
	// We use it to link keys from etcdTrx events to updaters. We use array of updaters, because OVSDB allows to specify
	// an array of <dbMonitor-request> objects for a monitored table
	key2Updaters Key2Updaters

	//revChecker revisionChecker
	tQueue  transactionsQueue
	handler *Handler
}

type queueElement struct {
	revision int64
	wg       *sync.WaitGroup
}

type transactionsQueue struct {
	queue *list.List
	mu    sync.Mutex
}

type ovsdbKeyValue struct {
	key common.Key
	row libovsdb.Row
}
type ovsdbNotificationEvent struct {
	createEvent bool
	modifyEvent bool
	kv          *ovsdbKeyValue
	prevKv      *ovsdbKeyValue
}

func etcd2ovsdbEvent(event *clientv3.Event, log logr.Logger) (*ovsdbNotificationEvent, error) {
	mvccpbEvent := mvccpb.Event(*event)
	ovsdbEvent := ovsdbNotificationEvent{
		createEvent: event.IsCreate(),
		modifyEvent: event.IsModify(),
	}
	key, err := common.ParseKey(string(event.Kv.Key))
	if err != nil {
		log.Error(err, "ParseKey error", "key", string(event.Kv.Key), "event", mvccpbEvent.String())
		return nil, err
	}
	if !ovsdbEvent.createEvent && !ovsdbEvent.modifyEvent {
		// delete event
		ovsdbEvent.kv = &ovsdbKeyValue{key: *key}
	} else {
		var row libovsdb.Row
		err = row.UnmarshalJSON(event.Kv.Value)
		if err != nil {
			log.Error(err, "cannot unmarshal JsonValue", "value", string(event.Kv.Value), "event", mvccpbEvent.String())
			return nil, err
		}
		ovsdbEvent.kv = &ovsdbKeyValue{key: *key, row: row}
	}
	if !ovsdbEvent.createEvent {
		var pRow libovsdb.Row
		err = pRow.UnmarshalJSON(event.PrevKv.Value)
		if err != nil {
			log.Error(err, "cannot unmarshal previous JsonValue", "value", string(event.PrevKv.Value), "event", mvccpbEvent.String())
			return nil, err
		}
		ovsdbEvent.prevKv = &ovsdbKeyValue{key: *key, row: pRow}
	}
	return &ovsdbEvent, nil
}

func newTQueue() transactionsQueue {
	return transactionsQueue{queue: list.New()}
}

func (tq *transactionsQueue) startTransaction() {
	tq.mu.Lock()
}

func (tq *transactionsQueue) abortTransaction() {
	tq.mu.Unlock()
}

func (tq *transactionsQueue) endTransaction(rev int64, wg *sync.WaitGroup) {
	// we are holding the lock
	qe := queueElement{revision: rev, wg: wg}
	tq.queue.PushBack(qe)
	tq.mu.Unlock()
}

func (tq *transactionsQueue) notificationSent(rev int64) {
	klog.V(7).Infof("notificationSent rev %d", rev)
	tq.mu.Lock()
	defer tq.mu.Unlock()
	klog.V(7).Infof("notificationSent rev %d, size %d", rev, tq.queue.Len())
	for tq.queue.Len() > 0 {
		element := tq.queue.Front()
		qElement := (element.Value).(queueElement)
		klog.V(7).Infof("notificationSent rev %d, qElement rev %d", rev, qElement.revision)
		if qElement.revision <= rev {
			tq.queue.Remove(element)
			qElement.wg.Done()
			klog.V(7).Infof("notificationSent rev %d called Done", rev)
		} else {
			return
		}
	}
}

func (tq *transactionsQueue) cleanUp() {
	// it is done at the end life of monitor, co we don't have to achieve lock
	for tq.queue.Len() > 0 {
		element := tq.queue.Front()
		qElement := (element.Value).(queueElement)
		qElement.wg.Done()
		tq.queue.Remove(element)
	}
}

func newMonitor(dbName string, handler *Handler, log logr.Logger) *dbMonitor {
	m := dbMonitor{
		log:          log,
		dataBaseName: dbName,
		handler:      handler,
		key2Updaters: Key2Updaters{},
		tQueue:       newTQueue(),
	}
	return &m
}

func (m *dbMonitor) addUpdaters(keyToUpdaters Key2Updaters) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, updaters := range keyToUpdaters {
		_, ok := m.key2Updaters[key]
		if !ok {
			m.key2Updaters[key] = []updater{}
		}
		for _, uNew := range updaters {
			for _, u1 := range m.key2Updaters[key] {
				if reflect.DeepEqual(uNew, u1) {
					continue
				}
			}
			m.key2Updaters[key] = append(m.key2Updaters[key], uNew)
		}
	}
}

func (m *dbMonitor) removeUpdaters(keys []common.Key, jsonValue string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		m.key2Updaters.removeUpdaters(key, jsonValue)
	}
}

// called under monitor.mu lock
func (m *dbMonitor) hasUpdaters() bool {
	return len(m.key2Updaters) > 0
}

func (m *dbMonitor) start() {
	go func() {
		m.log.V(6).Info("Start DB monitor", "dbName", m.dataBaseName)
		for wresp := range m.watchChannel {
			if wresp.Canceled {
				m.log.V(6).Info("DB monitor was canceled", "dbName", m.dataBaseName)
				m.cancelDbMonitor()
				return
			}
			m.log.V(6).Info("DB monitor get events", "dbName", m.dataBaseName, "events length", strconv.Itoa(len(wresp.Events)),
				"revision", strconv.FormatInt(wresp.Header.Revision, 10))
			m.notify(wresp.Events, wresp.Header.Revision)
		}
		m.log.V(6).Info("Start DB monitor ended", "dbName", m.dataBaseName)
	}()
}

func (hm *handlerMonitorData) notifier(ch *Handler) {
	// we need some time to allow to the monitor calls return data
	hm.log.V(6).Info("Notifier started", "jsonValue", hm.jsonValue)
	for {
		select {
		case <-ch.handlerContext.Done():
			hm.log.V(5).Info("Notifier ended", "jsonValue", hm.jsonValue)
			return

		case notificationEvent := <-hm.notificationChain:
			hm.log.V(7).Info("Notifier got event", "jsonValue", hm.jsonValue)
			if ch.handlerContext.Err() != nil {
				ch.mu.RLock()
				dbMonitor, ok := ch.monitors[hm.dataBaseName]
				ch.mu.RUnlock()
				if ok && dbMonitor != nil {
					dbMonitor.tQueue.cleanUp()
				}
				return
			}
			if notificationEvent.updates != nil {
				if hm.log.V(6).Enabled() {
					hm.log.V(6).Info("sending notification", "revision", notificationEvent.revision, "updates", notificationEvent.updates)
				} else {
					hm.log.V(5).Info("sending notification", "revision", notificationEvent.revision)
				}

				var err error
				switch hm.notificationType {
				case ovsjson.Update:
					err = ch.jrpcServer.Notify(ch.handlerContext, Update, []interface{}{hm.jsonValue, notificationEvent.updates})
				case ovsjson.Update2:
					err = ch.jrpcServer.Notify(ch.handlerContext, Update2, []interface{}{hm.jsonValue, notificationEvent.updates})
				case ovsjson.Update3:
					err = ch.jrpcServer.Notify(ch.handlerContext, Update3, []interface{}{hm.jsonValue, ovsjson.ZERO_UUID, notificationEvent.updates})
				}
				if err != nil {
					// TODO should we do something else
					hm.log.Error(err, "monitor notification failed")
				}
			}
			ch.mu.RLock()
			dbMonitor, ok := ch.monitors[hm.dataBaseName]
			ch.mu.RUnlock()
			if ok && dbMonitor != nil {
				dbMonitor.tQueue.notificationSent(notificationEvent.revision)
			} else {
				hm.log.V(5).Info("dataBase monitor is nil", "dataBaseName", hm.dataBaseName)
			}

		}
	}
}

func (m *dbMonitor) notify(events []*clientv3.Event, revision int64) {

	if len(events) == 0 {
		m.log.V(5).Info("there is no events, return")
		// we called here to release transaction queue, if there are elements there
		m.handler.notifyAll(revision)
	}
	ovsdbEvents := make([]*ovsdbNotificationEvent, 0, len(events))
	for _, event := range events {
		ovsdbEvent, err := etcd2ovsdbEvent(event, m.log)
		if err != nil {
			mvccpbEvent := mvccpb.Event(*event)
			m.log.Error(err, "etcd2ovsdbEvent returned", "event", mvccpbEvent.String())
			continue
		}
		ovsdbEvents = append(ovsdbEvents, ovsdbEvent)
	}
	m.log.V(5).Info("notify:", "notification revision", revision)
	result, err := m.prepareTableNotification(ovsdbEvents)
	if err != nil {
		// TODO what should I do here?
		m.log.Error(err, "prepareTableNotification failed")
		m.handler.notifyAll(revision)
	} else {
		if len(result) == 0 {
			m.log.V(5).Info("there is nothing to notify", "events", fmt.Sprintf("%+v", events))
			m.handler.notifyAll(revision)
			return
		}
		for jValue, tableUpdates := range result {
			// TODO if there are several monitors (jsonValues) can be a race condition to transaction returns
			m.log.V(7).Info("notify", "revision", revision, "table-update", tableUpdates)
			m.handler.notify(jValue, tableUpdates, revision)
		}
	}
}

func (m *dbMonitor) cancelDbMonitor() {
	m.cancel()
	jasonValues := map[string]string{}
	m.mu.Lock()
	for _, updaters := range m.key2Updaters {
		for _, updater := range updaters {
			jasonValues[updater.jasonValueStr] = updater.jasonValueStr
		}
	}
	m.key2Updaters = Key2Updaters{}
	m.mu.Unlock()
	m.tQueue.cleanUp()
	for jsonValue := range jasonValues {
		m.handler.monitorCanceledNotification(jsonValue)
	}
}

func mcrToUpdater(mcr ovsjson.MonitorCondRequest, jsonValue string, tableSchema *libovsdb.TableSchema, isV1 bool, log logr.Logger) *updater {
	if mcr.Select == nil {
		mcr.Select = &libovsdb.MonitorSelect{}
	}
	return &updater{mcr: mcr, jasonValueStr: jsonValue, isV1: isV1, tableSchema: tableSchema, log: log}
}

func (m *dbMonitor) prepareTableNotification(events []*ovsdbNotificationEvent) (map[string]ovsjson.TableUpdates, error) {
	result := map[string]ovsjson.TableUpdates{}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ev := range events {
		if ev.kv == nil {
			m.log.V(5).Info("empty etcdTrx event", "event", fmt.Sprintf("%+v", ev))
			continue
		}
		/*key, err := common.ParseKey(string(ev.Kv.Key))
		if err != nil {
			m.log.Error(err, "parseKey failed")
			continue
		}*/
		key := ev.kv.key
		updaters, ok := m.key2Updaters[key.ToTableKey()]
		if !ok {
			m.log.V(7).Info("no monitors for table path", "table-path", key.TableKeyString())
			continue
		}
		for _, upd := range updaters {
			rowUpdate, uuid, err := upd.prepareRowNotification(ev)
			if err != nil {
				m.log.Error(err, "prepareRowNotification failed", "updater", upd)
				continue
			}
			if rowUpdate == nil {
				// there is no updates
				m.log.V(7).Info("no updates for table path", "table-path", key.TableKeyString())
				continue
			}
			tableUpdates, ok := result[upd.jasonValueStr]
			if !ok {
				tableUpdates = ovsjson.TableUpdates{}
				result[upd.jasonValueStr] = tableUpdates
			}
			tableUpdate, ok := tableUpdates[key.TableName]
			if !ok {
				tableUpdate = ovsjson.TableUpdate{}
				tableUpdates[key.TableName] = tableUpdate
			}
			// check if there is a rowUpdate for the same uuid
			_, ok = tableUpdate[uuid]
			if ok {
				m.log.V(5).Info("duplicate event", "key", key.ShortString(), "table-update", tableUpdate[uuid], "row-update", rowUpdate)
			}
			tableUpdate[uuid] = *rowUpdate
		}
	}
	return result, nil
}

func (u *updater) prepareRowNotification(event *ovsdbNotificationEvent) (*ovsjson.RowUpdate, string, error) {
	if !event.modifyEvent { // the create or delete
		if event.createEvent {
			// Create event
			return u.prepareCreateRowUpdate(event)
		} else {
			// Delete event
			return u.prepareDeleteRowUpdate(event)
		}
	}
	// a modify event
	return u.prepareModifyRowUpdate(event)
}

func (u *updater) prepareDeleteRowUpdate(event *ovsdbNotificationEvent) (*ovsjson.RowUpdate, string, error) {
	if !libovsdb.MSIsTrue(u.mcr.Select.Delete) {
		return nil, "", nil
	}
	if !u.isV1 {
		// according to https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/#update2-notification,
		// "<row> is always a null object for delete updates."
		data, uuid, err := u.prepareRow(&event.prevKv.row)
		if err != nil {
			return nil, "", err
		}
		if data == nil {
			return nil, "", nil
		}
		return &ovsjson.RowUpdate{Delete: true}, uuid, nil
	}

	data, uuid, err := u.prepareRow(&event.prevKv.row)
	if err != nil {
		return nil, "", err
	}
	// for !u.isV1 we have returned before
	return &ovsjson.RowUpdate{Old: &data}, uuid, nil
}

func (u *updater) prepareCreateRowUpdate(event *ovsdbNotificationEvent) (*ovsjson.RowUpdate, string, error) {
	if !libovsdb.MSIsTrue(u.mcr.Select.Insert) {
		return nil, "", nil
	}
	data, uuid, err := u.prepareRow(&event.kv.row)
	if err != nil {
		return nil, "", err
	}
	if data == nil {
		return nil, "", nil
	}
	if !u.isV1 {
		return &ovsjson.RowUpdate{Insert: &data}, uuid, nil
	}
	return &ovsjson.RowUpdate{New: &data}, uuid, nil
}

func (u *updater) prepareModifyRowUpdate(event *ovsdbNotificationEvent) (*ovsjson.RowUpdate, string, error) {
	if !libovsdb.MSIsTrue(u.mcr.Select.Modify) {
		return nil, "", nil
	}
	modifiedRow, uuid, err := u.prepareRow(&event.kv.row)
	if err != nil {
		return nil, "", err
	}

	prevRow, prevUUID, err := u.prepareRow(&event.prevKv.row)
	if err != nil {
		return nil, "", err
	}

	if modifiedRow == nil && prevRow == nil {
		// the row is not selected
		return nil, "", nil
	}
	if modifiedRow == nil {
		if u.isV1 {
			u.log.V(1).Info("monitor version is V1, but modified row is nil", "event", event, "where", u.mcr.Where)
			return nil, "", fmt.Errorf("monitor version is V1, but modified row is nil, prevUUID %s", prevUUID)
		}
		return &ovsjson.RowUpdate{Delete: true}, prevUUID, nil
	}
	if prevRow == nil {
		if u.isV1 {
			u.log.V(1).Info("monitor version is V1, but previous row is nil", "event", event, "where", u.mcr.Where)
			return nil, "", fmt.Errorf("monitor version is V1, but previous row is nil, UUID %s", uuid)
		}
		return &ovsjson.RowUpdate{Insert: &modifiedRow}, uuid, nil
	}
	if uuid != prevUUID {
		err := fmt.Errorf("UUID was changed key=%s, prev uuid=%q, new uuid=%q", event.kv.key.String(), prevUUID, uuid)
		u.log.Error(err, "", "prevRow", event.prevKv.row, "newRow", event.kv.row)
		return nil, "", err
	}
	deltaRow, err := u.compareModifiedRows(modifiedRow, prevRow)
	if err != nil {
		return nil, "", err
	}
	klog.V(7).Infof("deltaRow size is %d", len(deltaRow))
	if !u.isV1 {
		return &ovsjson.RowUpdate{Modify: &deltaRow}, uuid, nil
	}
	return &ovsjson.RowUpdate{New: &modifiedRow, Old: &deltaRow}, uuid, nil
}

func (u *updater) compareModifiedRows(modifiedRow, prevRow map[string]interface{}) (map[string]interface{}, error) {
	deltaRow := map[string]interface{}{}
	for columnName, columnSchema := range u.tableSchema.Columns {
		prevValue, prevOK := prevRow[columnName]
		newValue, newOK := modifiedRow[columnName]
		if reflect.DeepEqual(newValue, prevValue) {
			continue
		}
		var deltaValue interface{}
		if u.isV1 {
			if prevOK {
				deltaValue = prevValue
			}
		} else {
			// V2
			if columnSchema.Type == libovsdb.TypeMap {
				deltaMap, err := u.compareMaps(newValue, prevValue, columnSchema)
				if err != nil {
					return deltaRow, err
				}
				if len(deltaMap.GoMap) > 0 {
					deltaValue = deltaMap
				}
			} else if columnSchema.Type == libovsdb.TypeSet && columnSchema.TypeObj.Max != 1 {
				deltaSet, err := u.compareSets(newValue, prevValue, columnSchema)
				if err != nil {
					return deltaRow, err
				}
				if len(deltaSet.GoSet) > 0 {
					deltaValue = deltaSet
				}
			} else {
				if newOK {
					deltaValue = newValue
				} else if prevOK {
					deltaValue = prevValue
				}
			}
		}
		if deltaValue != nil {
			if u.isV1 {
				deltaRow[columnName] = prevRow[columnName]
			} else {
				deltaRow[columnName] = deltaValue
			}
		}
	}

	return deltaRow, nil
}

func (u *updater) compareMaps(data, prevData interface{}, columnSchema *libovsdb.ColumnSchema) (*libovsdb.OvsMap, error) {
	deltaMap := libovsdb.OvsMap{GoMap: make(map[interface{}]interface{})}
	v, err := columnSchema.UnmarshalMap(data)
	if err != nil {
		return nil, fmt.Errorf("cannot convert column %v to map: %v", data, err)
	}
	newMap := v.(libovsdb.OvsMap)

	v, err = columnSchema.UnmarshalMap(prevData)
	if err != nil {
		return nil, fmt.Errorf("cannot convert prevData column %v to map: %v", prevData, err)
	}
	prevMap := v.(libovsdb.OvsMap)
	// check new values
	for k, v := range newMap.GoMap {
		pv, ok := prevMap.GoMap[k]
		if !ok || !reflect.DeepEqual(v, pv) {
			deltaMap.GoMap[k] = v
		}
	}
	// we need to find all keys that were in the prev map, but are not in the new one
	for pk, pv := range prevMap.GoMap {
		if _, ok := deltaMap.GoMap[pk]; ok {
			continue
		}
		if _, ok := newMap.GoMap[pk]; !ok {
			deltaMap.GoMap[pk] = pv
		}
	}
	return &deltaMap, nil
}

func (u *updater) compareSets(data, prevData interface{}, columnSchema *libovsdb.ColumnSchema) (*libovsdb.OvsSet, error) {
	v, err := columnSchema.UnmarshalSet(data)
	if err != nil {
		return nil, fmt.Errorf("cannot convert column %v to set: %v", data, err)
	}
	newSet := v.(libovsdb.OvsSet)
	v, err = columnSchema.UnmarshalSet(prevData)
	if err != nil {
		return nil, fmt.Errorf("cannot convert prevData column %v to set: %v", prevData, err)
	}
	prevSet := v.(libovsdb.OvsSet)
	deltaSet := setsDifference(newSet, prevSet)
	return &deltaSet, nil
}

func (u *updater) prepareInitialRow(row libovsdb.Row) (*ovsjson.RowUpdate, string, error) {
	if !libovsdb.MSIsTrue(u.mcr.Select.Initial) {
		return nil, "", nil
	}
	data, uuid, err := u.prepareRow(&row)
	if err != nil {
		return nil, "", err
	}
	if !u.isV1 {
		return &ovsjson.RowUpdate{Initial: &data}, uuid, nil
	}
	return &ovsjson.RowUpdate{New: &data}, uuid, nil
}

func (u *updater) deleteUnselectedColumns(data map[string]interface{}) map[string]interface{} {
	if u.mcr.Columns == nil {
		return data
	}
	newData := map[string]interface{}{}
	for _, column := range *u.mcr.Columns {
		value, ok := data[column]
		if ok {
			newData[column] = value
		}
	}
	return newData
}

func (u *updater) copySelectedColumns(data map[string]interface{}) map[string]interface{} {
	newData := map[string]interface{}{}
	if u.mcr.Columns == nil {
		for k, v := range data {
			newData[k] = v
		}
		return newData
	}
	for _, column := range *u.mcr.Columns {
		value, ok := data[column]
		if ok {
			newData[column] = value
		}
	}
	return newData
}

func unmarshalData(data []byte) (map[string]interface{}, error) {
	obj := map[string]interface{}{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (u *updater) isRowAppearOnWhere(data map[string]interface{}) (bool, error) {
	if u.mcr.Where == nil {
		return true, nil
	}
	checkCondition := func(condition []interface{}) (bool, error) {
		log := klogr.New() // TODO: propagate real logger instead of this generic one
		res, err := NewCondition(u.tableSchema, condition, log)
		if err != nil {
			return false, err
		}
		cond, err := res.Compare(&data)
		if err != nil {
			return false, err
		}
		return cond, nil
	}
	var cond bool
	var err error
	for i := 0; i < len(*u.mcr.Where); i++ {
		switch condition := ((*u.mcr.Where)[i]).(type) {
		case []interface{}:
			cond, err = checkCondition(condition)
		case [3]interface{}:
			cond, err = checkCondition(condition[:])
		case bool:
			cond = condition
		default:
			return false, fmt.Errorf("wrong type %T %v should be a [3]interface{} or []interface{} or bool", condition, condition)
		}
		if err != nil {
			return false, err
		}
		if cond == false {
			return false, nil
		}
	}
	return true, nil
}

func (u *updater) prepareRow(row *libovsdb.Row) (map[string]interface{}, string, error) {
	data := row.Fields
	res, err := u.isRowAppearOnWhere(data)
	if err != nil {
		return nil, "", err
	}
	if res == false {
		return nil, "", nil
	}
	uuid, err := row.GetUUID()
	if err != nil {
		return nil, "", err
	}
	data = u.copySelectedColumns(data)
	delete(data, libovsdb.ColUuid)
	return data, uuid.GoUUID, nil
}

// setsDifference returns a delta between 2 sets. It assumes that there is no duplicate elements in the sets.
func setsDifference(set1 libovsdb.OvsSet, set2 libovsdb.OvsSet) libovsdb.OvsSet {
	var diff libovsdb.OvsSet
	m := make(map[interface{}]bool)

	for _, item := range set2.GoSet {
		m[item] = true
	}

	for _, item := range set1.GoSet {
		if _, ok := m[item]; !ok {
			diff.GoSet = append(diff.GoSet, item)
		} else {
			delete(m, item)
		}
	}
	for item := range m {
		diff.GoSet = append(diff.GoSet, item)
	}
	return diff
}
