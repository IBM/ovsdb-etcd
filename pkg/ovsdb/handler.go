package ovsdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"

	"github.com/creachadair/jrpc2"
	"github.com/go-logr/logr"
	"github.com/lithammer/shortuuid/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

type JrpcServer interface {
	Wait() error
	Stop()
	Notify(ctx context.Context, method string, params interface{}) error
}

type Handler struct {
	log logr.Logger

	db         Databaser
	etcdClient *clientv3.Client

	jrpcServer     JrpcServer
	handlerContext context.Context
	clientCon      net.Conn
	closed         bool // false by default
	mu             sync.RWMutex

	// dbName->dbMonitor
	monitors map[string]*dbMonitor
	// json-value string to handler monitor related data
	handlerMonitorData map[string]handlerMonitorData

	databaseLocks sync.Map
}

func (ch *Handler) Transact(ctx context.Context, params []interface{}) (interface{}, error) {
	req := jrpc2.InboundRequest(ctx)
	id := ""
	if !req.IsNotification() {
		id = req.ID()
	}
	log := ch.log.WithValues("id", id, "txnid", shortuuid.New())
	log.V(5).Info("transact", "params", params)
	if ch.closed {
		log.V(5).Info("transact request, the handler is closed")
		// prevents old transactions
		return nil, nil
	}
	ovsReq, err := libovsdb.NewTransact(params)
	if err != nil {
		return nil, err
	}
	schema, ok := ch.db.GetDBSchema(ovsReq.DBName)
	if !ok {
		err := errors.New(E_INTERNAL_ERROR)
		log.V(1).Info("Unknown schema", "dbName", ovsReq.DBName)
		return nil, err
	}
	dbCache, err := ch.db.GetDBCache(ovsReq.DBName)
	if !ok {
		err := errors.New(E_INTERNAL_ERROR)
		log.V(1).Info("Database cache is not created", "dbName", ovsReq.DBName)
		return nil, err
	}
	txn, err := NewTransaction(ctx, ch.etcdClient, ovsReq, dbCache, schema, log)
	if err != nil {
		return nil, errors.New(E_INTERNAL_ERROR)
	}
	ch.mu.RLock()
	monitor, thereIsMonitor := ch.monitors[txn.request.DBName]
	ch.mu.RUnlock()
	txn.log.V(7).Info("before tables lock")
	// temporary solution to provide consistency
	err = txn.lockTables()
	if err != nil {
		return nil, err
	}
	txn.log.V(7).Info("got tables lock")
	if thereIsMonitor {
		monitor.tQueue.startTransaction()
	}
	rev, errC := txn.Commit()
	err = txn.unLockTables()
	txn.log.V(7).Info("tables unlocked")
	if errC != nil || err != nil {
		if thereIsMonitor {
			monitor.tQueue.abortTransaction()
		}
		if errC != nil {
			txn.log.Error(errC, "commit returned")
			return nil, errC
		}
		txn.log.Error(err, "unlock returned")
		return nil, err
	}
	if thereIsMonitor {
		// we have to guarantee that a new monitor call if it runs concurrently with the transaction, returns first
		var wg sync.WaitGroup
		wg.Add(1)
		monitor.tQueue.endTransaction(rev, &wg)
		log.V(5).Info("transact added", "etcdTrx revision", rev)
		wg.Wait()
	}

	log.V(5).Info("transact response", "response", txn.response, "etcdTrx revision", rev)
	return txn.response.Result, nil
}

func (ch *Handler) Cancel(ctx context.Context, param interface{}) (interface{}, error) {
	ch.log.V(5).Info("cancel request", "param", param)

	return "{Cancel}", nil
}

func (ch *Handler) Monitor(ctx context.Context, params []interface{}) (interface{}, func(), error) {
	ch.log.V(5).Info("monitor request", "params", params)
	updatersMap, err := ch.addMonitor(params, ovsjson.Update)
	if err != nil {
		ch.log.Error(err, "monitor rquest failed", "params", params)
		return nil, nil, err
	}
	data, err := ch.getMonitoredData(params[0].(string), updatersMap)
	if err != nil {
		ch.log.Error(err, "failed to get monitored data")
		ch.removeMonitor(params[1], false)
		return nil, nil, err
	}
	jsonValueString := jsonValueToString(params[1])
	ch.log.V(5).Info("monitor response", "jsonValue", params[1], "data", data)
	return data, func() { ch.startNotifier(jsonValueString) }, nil
}

func (ch *Handler) MonitorCancel(ctx context.Context, param interface{}) (interface{}, error) {
	ch.log.V(5).Info("monitorCancel", "param", param)
	err := ch.removeMonitor(param, true)
	if err != nil {
		return nil, err
	}
	return "{}", nil
}

func (ch *Handler) Lock(ctx context.Context, param interface{}) (interface{}, error) {
	ch.log.V(5).Info("lock request", "param", param)
	id, err := common.ParamsToString(param)
	if err != nil {
		ch.log.Error(err, "Lock, param parsing", "param", param)
		return nil, err
	}
	var myLock Locker
	locI, ok := ch.databaseLocks.Load(id)
	if ok {
		myLock, ok = locI.(Locker)
		if !ok {
			err = fmt.Errorf("cannot transform to Logger, %T", locI)
			ch.log.Error(err, "")
			return nil, err
		}
	} else {
		myLock, err = ch.db.GetLock(ch.handlerContext, id)
		if err != nil {
			ch.log.Error(err, "lock failed", "lockid", id)
			return nil, err
		}
		ch.log.V(6).Info("new etcd lock created", "id", id)
		// validate that no other locks
		otherLock, ok := ch.databaseLocks.Load(id)
		if !ok {
			ch.databaseLocks.Store(id, myLock)
		} else {
			ch.log.V(6).Info("there is another lock in the local map", "id", id)
			// What should we do ?
			myLock.cancel()
			myLock, ok = otherLock.(Locker)
			if !ok {
				err = fmt.Errorf("cannot transform to Logger, %T", otherLock)
				ch.log.Error(err, "")
				return nil, err
			}
		}
	}
	err = myLock.tryLock()
	if err == nil {
		ch.log.V(5).Info("lock request returned - \"locked: true\"", "id", id)
		return map[string]bool{"locked": true}, nil
	} else if err != concurrency.ErrLocked {
		ch.log.Error(err, "lock failed", "lockid", id)
		// TOD is it correct?
		return nil, err
	}
	go func() {
		err = myLock.lock()
		if err == nil {
			// Send notification
			ch.log.V(5).Info("lock succeeded", "id", id)
			if err := ch.jrpcServer.Notify(ch.handlerContext, "locked", []string{id}); err != nil {
				klog.Errorf("notification %v\n", err)
				return
			}
		} else {
			ch.log.Error(err, "lock failed", "id", id)
		}
	}()
	ch.log.V(5).Info("lock request returned - \"locked: false\"", "id", id)
	return map[string]bool{"locked": false}, nil
}

func (ch *Handler) Unlock(ctx context.Context, param interface{}) (interface{}, error) {
	ch.log.V(5).Info("unlock request", "param", param)
	id, err := common.ParamsToString(param)
	if err != nil {
		return ovsjson.EmptyStruct{}, err
	}
	iLock, ok := ch.databaseLocks.LoadAndDelete(id)
	if !ok {
		ch.log.V(4).Info("unlock: can't find lock", "lockid", id)
		return ovsjson.EmptyStruct{}, nil
	}
	myLock, ok := iLock.(Locker)
	if !ok {
		err = fmt.Errorf("cannot transform to Logger, %T", iLock)
		ch.log.Error(err, "")
		return nil, err
	}
	myLock.cancel()
	return ovsjson.EmptyStruct{}, nil
}

func (ch *Handler) Steal(ctx context.Context, param interface{}) (interface{}, error) {
	ch.log.V(5).Info("steal request", "param", param)
	// TODO
	return "{Steal}", nil
}

func (ch *Handler) MonitorCond(ctx context.Context, params []interface{}) (interface{}, func(), error) {
	ch.log.V(5).Info("monitorCond request", "params", params)
	updatersMap, err := ch.addMonitor(params, ovsjson.Update2)
	if err != nil {
		ch.log.Error(err, "monitorCond from remote")
		return nil, nil, err
	}
	data, err := ch.getMonitoredData(params[0].(string), updatersMap)
	if err != nil {
		ch.log.Error(err, "failed to get monitored data")
		ch.removeMonitor(params[1], false)
		return nil, nil, err
	}
	jsonValueString := jsonValueToString(params[1])
	ch.log.V(5).Info("monitorCond response", "jsonValue", params[1], "data", data)
	return data, func() { ch.startNotifier(jsonValueString) }, nil
}

func (ch *Handler) MonitorCondChange(ctx context.Context, params []interface{}) (interface{}, error) {
	ch.log.V(5).Info("monitorCondChange request", "params", params)
	if len(params) != 3 {
		err := fmt.Errorf("wrong params length for MonitorCondChange %d , params %v", len(params), params)
		ch.log.Error(err, "monitorCondChange request")
		return nil, err
	}
	oldJsonValue := params[0]
	newJsonValue := params[1]
	mcrs := map[string][]ovsjson.MonitorCondRequest{}
	buf, err := json.Marshal(params[2])
	if err != nil {
		ch.log.Error(err, "marshal conditional request returned")
		return nil, err
	}
	if err := json.Unmarshal(buf, &mcrs); err != nil {
		obj := map[string]ovsjson.MonitorCondRequest{}
		if err := json.Unmarshal(buf, &obj); err != nil {
			return nil, fmt.Errorf("unmarshal dbMonitor condition requests returned: %v", err)
		}
		mcrs = map[string][]ovsjson.MonitorCondRequest{}
		for k, v := range obj {
			mcrs[k] = []ovsjson.MonitorCondRequest{v}
		}
	}
	if reflect.DeepEqual(oldJsonValue, newJsonValue) {
		ch.log.V(5).Info("MonitorCondChange, update existing monitor")
		jsonValueString := jsonValueToString(oldJsonValue)
		ch.mu.Lock()
		defer ch.mu.Unlock()
		monitorData, ok := ch.handlerMonitorData[jsonValueString]
		if !ok {
			err := fmt.Errorf("unknown monitor")
			ch.log.Error(err, "update unexisting dbMonitor", "jsonValue", oldJsonValue)
			return nil, err
		}
		dbName := monitorData.dataBaseName
		monitor, ok := ch.monitors[dbName]
		if !ok {
			ch.log.V(5).Info("MonitorCondChange there is no monitor", "dbname", monitorData.dataBaseName)
		}
		databaseSchema, ok := ch.db.GetDBSchema(dbName)
		if !ok {
			return nil, fmt.Errorf("there is no databaseSchema for %s", dbName)
		}
		ch.log.V(5).Info("MonitorCondChange 2")
		for tableName, mcrArray := range mcrs {
			key := common.NewTableKey(dbName, tableName)
			_, ok := monitor.key2Updaters[key]
			if !ok {
				ch.log.V(6).Info("MonitorCondChange adding new updater for a new table")
				monitorData.updatersKeys = append(monitorData.updatersKeys, key)
			}
			ch.log.V(6).Info("MonitorCondChange", "table", tableName, "mcr", mcrArray)
			var updaters []updater
			tableSchema, err := databaseSchema.LookupTable(tableName)
			if err != nil {
				return nil, err
			}
			ch.log.V(5).Info("MonitorCondChange 2", "tableName", tableName)
			for _, mcr := range mcrArray {
				updater := mcrToUpdater(mcr, jsonValueString, tableSchema, monitorData.notificationType == ovsjson.Update, ch.log)
				updaters = append(updaters, *updater)
			}
			ch.monitors[dbName].key2Updaters[key] = updaters
		}
	}
	ch.log.V(5).Info("MonitorCondChange END")
	return ovsjson.EmptyStruct{}, nil
}

func (ch *Handler) MonitorCondSince(ctx context.Context, params []interface{}) (interface{}, func(), error) {
	ch.log.V(5).Info("MonitorCondSince request", "params", params)
	updatersMap, err := ch.addMonitor(params, ovsjson.Update3)
	if err != nil {
		ch.log.Error(err, "MonitorCondSince failed")
		return nil, nil, err
	}

	data, err := ch.getMonitoredData(params[0].(string), updatersMap)
	if err != nil {
		ch.log.Error(err, "failed to get monitored data")
		ch.removeMonitor(params[1], false)
		return nil, nil, err
	}
	jsonValueString := jsonValueToString(params[1])
	ch.log.V(5).Info("MonitorCondSince response", "jsonValue", params[1], "data", fmt.Sprintf("%v", data))
	return []interface{}{false, ovsjson.ZERO_UUID, data}, func() { ch.startNotifier(jsonValueString) }, nil
}

func (ch *Handler) SetDbChangeAware(ctx context.Context, param interface{}) interface{} {
	ch.log.V(5).Info("SetDbChangeAware request", "param", param)
	return ovsjson.EmptyStruct{}
}

// RFC 7047 section 4.1.11
// Can be used by both clients and servers to verify the liveness of a database connection.
// "params": JSON array with any contents
// Returns : "result": same as "params"
func (ch *Handler) Echo(ctx context.Context, param interface{}) interface{} {
	ch.log.V(5).Info("Echo request", "param", param)
	return param
}

func NewHandler(tctx context.Context, db Databaser, cli *clientv3.Client, log logr.Logger) *Handler {
	return &Handler{
		handlerContext:     tctx,
		db:                 db,
		databaseLocks:      sync.Map{},
		handlerMonitorData: map[string]handlerMonitorData{},
		etcdClient:         cli,
		monitors:           map[string]*dbMonitor{},
		log:                log.WithValues(),
	}
}

func (ch *Handler) Cleanup() error {
	ch.log.V(5).Info("CLEAN UP do something")
	ch.closed = true
	ch.databaseLocks.Range(func(key, value interface{}) bool {
		mLock, ok := value.(Locker)
		if !ok {
			err := fmt.Errorf("cannot transform to Logger, value type %T, key %s", value, key)
			ch.log.Error(err, "")
		}
		mLock.unlock()
		ch.databaseLocks.Delete(key)
		return true
	})

	for _, monitor := range ch.monitors {
		monitor.cancelDbMonitor()
	}
	return nil
}

func (ch *Handler) SetConnection(jrpcSerer JrpcServer, clientCon net.Conn) {
	ch.jrpcServer = jrpcSerer
	ch.clientCon = clientCon
	ch.log = ch.log.WithValues("client", ch.GetClientAddress())
}

func (ch *Handler) notify(jsonValueString string, updates ovsjson.TableUpdates, revision int64) {
	ch.mu.RLock()
	hmd, ok := ch.handlerMonitorData[jsonValueString]
	ch.mu.RUnlock()
	if !ok {
		err := fmt.Errorf("there is no handler monitor data for %s", jsonValueString)
		ch.log.Error(err, "notify")
		// should we notify all here?
		// ch.notifyAll(revision)
		return
	}
	if klog.V(7).Enabled() {
		ch.log.V(7).Info("monitor notification", "jsonValue", hmd.jsonValue, "revision", revision, "updates", updates)
	} else {
		ch.log.V(5).Info("monitor notification", "jsonValue", hmd.jsonValue, "revision", revision)
	}
	hmd.notificationChain <- notificationEvent{updates: updates, revision: revision}
	ch.log.V(5).Info("monitor notification, event was sent", "jsonValue", hmd.jsonValue, "revision", revision)
}

func (ch *Handler) notifyAll(revision int64) {
	ch.log.V(7).Info("notifyAll", "revision", strconv.FormatInt(revision, 10))
	// TODO optimize
	hmdArray := []handlerMonitorData{}
	ch.mu.RLock()
	for _, hmd := range ch.handlerMonitorData {
		hmdArray = append(hmdArray, hmd)
	}
	ch.mu.RUnlock()
	for _, hmd := range hmdArray {
		hmd.notificationChain <- notificationEvent{revision: revision}
	}
	ch.log.V(7).Info("notifyAll finished", strconv.FormatInt(revision, 10))
}

func (ch *Handler) monitorCanceledNotification(jsonValue interface{}) {
	if ch.closed {
		ch.log.V(5).Info("monitorCanceledNotification", "jsonValue", jsonValue)
		err := ch.jrpcServer.Notify(ch.handlerContext, MONITOR_CANCELED, jsonValue)
		if err != nil {
			// Usually we get this error because a client closed his connection, so we don't need to inform it as an error
			ch.log.V(6).Info("monitorCanceledNotification failed", "error", err.Error())
		}
	}
}

func (ch *Handler) removeMonitor(jsonValue interface{}, notify bool) error {
	ch.log.V(5).Info("removeMonitor failed", "jsonValue", jsonValue)

	jsonValueString := jsonValueToString(jsonValue)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	monitorData, ok := ch.handlerMonitorData[jsonValueString]
	if !ok {
		ch.log.V(5).Info("removing unexisting dbMonitor", "jsonValue", jsonValue)
		err := fmt.Errorf("unknown monitor")
		return err
	}
	monitor, ok := ch.monitors[monitorData.dataBaseName]
	if !ok {
		ch.log.V(5).Info("there is no monitor", "dbname", monitorData.dataBaseName)
	}

	monitor.removeUpdaters(monitorData.updatersKeys, jsonValueString)

	if !monitor.hasUpdaters() {
		monitor.cancel()
		delete(ch.monitors, monitorData.dataBaseName)
	}
	delete(ch.handlerMonitorData, jsonValueString)
	if notify {
		ch.monitorCanceledNotification(jsonValue)
	}
	return nil
}

func (ch *Handler) addMonitor(params []interface{}, notificationType ovsjson.UpdateNotificationType) (Key2Updaters, error) {

	cmpr, err := parseCondMonitorParameters(params)
	if err != nil {
		return nil, err
	}
	if len(cmpr.DatabaseName) == 0 {
		return nil, fmt.Errorf("monitored dataBase name is empty")
	}

	jsonValueString := jsonValueToString(cmpr.JsonValue)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if _, ok := ch.handlerMonitorData[jsonValueString]; ok {
		return nil, fmt.Errorf("duplicate monitor ID")
	}
	databaseSchema, ok := ch.db.GetDBSchema(cmpr.DatabaseName)
	if !ok {
		return nil, fmt.Errorf("there is no databaseSchema for %s", cmpr.DatabaseName)
	}
	updatersMap := Key2Updaters{}
	var updatersKeys []common.Key
	for tableName, mcrs := range cmpr.MonitorCondRequests {
		var updaters []updater
		tableSchema, err := databaseSchema.LookupTable(tableName)
		if err != nil {
			klog.Errorf("%v", err)
			return nil, err
		}
		for _, mcr := range mcrs {
			updater := mcrToUpdater(mcr, jsonValueString, tableSchema, notificationType == ovsjson.Update, ch.log)
			updaters = append(updaters, *updater)
		}
		key := common.NewTableKey(cmpr.DatabaseName, tableName)
		updatersMap[key] = updaters
		updatersKeys = append(updatersKeys, key)
	}
	log := ch.log.WithValues("jsonValue", cmpr.JsonValue)
	monitor, ok := ch.monitors[cmpr.DatabaseName]
	if !ok {
		monitor = ch.db.CreateMonitor(cmpr.DatabaseName, ch, log)
		monitor.start()
		ch.monitors[cmpr.DatabaseName] = monitor
	}
	monitor.addUpdaters(updatersMap)
	ch.handlerMonitorData[jsonValueString] = handlerMonitorData{
		log:               log,
		dataBaseName:      cmpr.DatabaseName,
		notificationType:  notificationType,
		updatersKeys:      updatersKeys,
		jsonValue:         cmpr.JsonValue,
		notificationChain: make(chan notificationEvent),
	}

	return updatersMap, nil
}

func (ch *Handler) startNotifier(jsonValue string) {
	ch.log.V(6).Info("start monitor notifier", "jsonValue", jsonValue)
	ch.mu.RLock()
	hmd, ok := ch.handlerMonitorData[jsonValue]
	ch.mu.RUnlock()
	if !ok {
		ch.log.V(5).Info("there is no notifier", "jsonValue", jsonValue)
	} else {
		go hmd.notifier(ch)
	}

}

func (ch *Handler) getMonitoredData(dbName string, updatersMap Key2Updaters) (ovsjson.TableUpdates, error) {
	keys := []common.Key{}
	for tableKey, updaters := range updatersMap {
		if len(updaters) == 0 {
			// nothing to update
			ch.log.V(5).Info("there is no updaters", "for table", tableKey.String())
			continue
		}
		// validate that Initial is required
		reqInitial := false
		for _, updater := range updaters {
			reqInitial = reqInitial || libovsdb.MSIsTrue(updater.mcr.Select.Initial)
			if reqInitial {
				break
			}
		}
		if reqInitial {
			keys = append(keys, tableKey)
		}
	}
	resp, err := ch.db.GetData(keys)
	if err != nil {
		return nil, err
	}
	returnData := ovsjson.TableUpdates{}
	for _, opRes := range resp.Responses {
		rangeResp := opRes.GetResponseRange()
		for _, kv := range rangeResp.Kvs {
			key, err := common.ParseKey(string(kv.Key))
			if err != nil {
				ch.log.Error(err, "parse failed", "key", string(kv.Key))
				return nil, err
			}
			tableKey := key.ToTableKey()
			updaters := updatersMap[tableKey]
			for _, updater := range updaters {
				row, uuid, err := updater.prepareInitialRow(&kv.Value)
				if err != nil {
					ch.log.Error(err, "prepareInitialRow returned")
					return nil, err
				}
				// TODO merge
				tableUpdate, ok := returnData[tableKey.TableName]
				if !ok {
					tableUpdate = ovsjson.TableUpdate{}
					returnData[tableKey.TableName] = tableUpdate
				}
				tableUpdate[uuid] = *row
			}
		}
	}
	ch.log.V(6).Info("getMonitoredData completed", "revision", resp.Header.Revision, "data", returnData)
	return returnData, nil
}

func (ch *Handler) GetClientAddress() string {
	if ch.clientCon != nil {
		return ch.clientCon.RemoteAddr().String()
	}
	return ""
}

func parseCondMonitorParameters(params []interface{}) (*ovsjson.CondMonitorParameters, error) {
	l := len(params)
	if l < 2 || l > 4 {
		err := fmt.Errorf("wrong length of condition dbMonitor parameters: %d", l)
		klog.Errorf("parseCondMonitorParameters %v params = %v", err, params)
		return nil, err
	}
	cmp := ovsjson.CondMonitorParameters{}
	var ok bool
	cmp.DatabaseName, ok = params[0].(string)
	if !ok {
		err := fmt.Errorf("parseCondMonitorParameters, cannot assert dbname interface (type %T, value %v) to string", params[0], params[0])
		klog.Errorf("%v", err)
		return nil, err
	}
	cmp.JsonValue = params[1]
	buf, err := json.Marshal(params[2])
	if err != nil {
		klog.Errorf("marshal dbMonitor conditional request returned %v", err)
		return nil, err
	}
	if err := json.Unmarshal(buf, &cmp.MonitorCondRequests); err != nil {
		obj := map[string]ovsjson.MonitorCondRequest{}
		if err := json.Unmarshal(buf, &obj); err != nil {
			return nil, fmt.Errorf("unmarshal dbMonitor condition requests returned: %v", err)
		}
		cmp.MonitorCondRequests = map[string][]ovsjson.MonitorCondRequest{}
		for k, v := range obj {
			cmp.MonitorCondRequests[k] = []ovsjson.MonitorCondRequest{v}
		}
	}
	if l == 4 {
		str, ok := params[3].(string)
		if !ok {
			err := fmt.Errorf("parseCondMonitorParameters, cannot assert last txn ID interface (type %T, value %v) to string", params[3], params[3])
			klog.Errorf("%v", err)
			return nil, err
		}
		cmp.LastTxnID = &str
	}
	return &cmp, nil
}

func jsonValueToString(jsonValue interface{}) string {
	return fmt.Sprintf("%v", jsonValue)
}
