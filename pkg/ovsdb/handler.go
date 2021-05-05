package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

type ClientConnection interface {
	Wait() error
	Stop()
	Notify(ctx context.Context, method string, params interface{}) error
}

type Handler struct {
	db         Databaser
	etcdClient *clientv3.Client

	connection     ClientConnection
	handlerContext context.Context

	mu sync.Mutex

	// jsonValue -> handlerMonitorData
	monitors      map[interface{}]handlerMonitorData
	databaseLocks map[string]Locker
}

func (ch *Handler) Transact(ctx context.Context, param []interface{}) (interface{}, error) {
	klog.V(5).Infof("Transact request, parameters %v", param)
	req, err := libovsdb.NewTransact(param)
	if err != nil {
		return nil, err
	}
	txn := NewTransaction(ch.etcdClient, req)
	txn.Commit()
	return txn.response.Result, nil
}

func (ch *Handler) Cancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Cancel request, parameters %v", param)

	return "{Cancel}", nil
}

func (ch *Handler) Monitor(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("Monitor request, parameters %v", param)
	if err := ch.monitor(param, ovsjson.Update); err != nil {
		return nil, err
	}
	return ch.getMonitoredData(param.DatabaseName, param.MonitorCondRequests, true)
}

func (ch *Handler) MonitorCancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCancel request, parameters %v", param)
	jsonValue := param

	ch.mu.Lock()
	defer ch.mu.Unlock()
	monitorHandler, ok := ch.monitors[jsonValue]
	if !ok {
		return nil, fmt.Errorf("unknown monitor")
	}
	ch.db.RemoveMonitors(monitorHandler.dataBaseName, monitorHandler.updaters, handlerKey{handler: ch, jsonValue: jsonValue})
	delete(ch.monitors, jsonValue)
	return "{}", nil
}

func (ch *Handler) Lock(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Lock request, parameters %v", param)
	id, err := common.ParamsToString(param)
	if err != nil {
		return map[string]bool{"locked": false}, err
	}
	ch.mu.Lock()
	myLock, ok := ch.databaseLocks[id]
	ch.mu.Unlock()
	if !ok {
		myLock, err = ch.db.GetLock(ch.handlerContext, id)
		if err != nil {
			klog.Warningf("Lock returned error %v\n", err)
			return nil, err
		}
		ch.mu.Lock()
		// validate that no other locks
		otherLock, ok := ch.databaseLocks[id]
		if !ok {
			ch.databaseLocks[id] = myLock
		} else {
			// What should we do ?
			myLock.cancel()
			myLock = otherLock
		}
		ch.mu.Unlock()
	}
	err = myLock.tryLock()
	if err == nil {
		return map[string]bool{"locked": true}, nil
	} else if err != concurrency.ErrLocked {
		klog.Errorf("Locked %s got error %v", id, err)
		// TOD is it correct?
		return nil, err
	}
	go func() {
		err = myLock.lock()
		if err == nil {
			// Send notification
			klog.V(5).Infoln("%s Locked", id)
			if err := ch.connection.Notify(ch.handlerContext, "locked", []string{id}); err != nil {
				klog.Errorf("notification %v\n", err)
				return
			}
		} else {
			klog.Errorf("Lock %s error %v\n", id, err)
		}
	}()
	return map[string]bool{"locked": false}, nil
}

func (ch *Handler) Unlock(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Unlock request, parameters %v", param)
	id, err := common.ParamsToString(param)
	if err != nil {
		return ovsjson.EmptyStruct{}, err
	}
	ch.mu.Lock()
	myLock, ok := ch.databaseLocks[id]
	delete(ch.databaseLocks, id)
	ch.mu.Unlock()
	if !ok {
		klog.V(4).Infof("Unlock non existing lock %s", id)
		return ovsjson.EmptyStruct{}, nil
	}
	myLock.cancel()
	return ovsjson.EmptyStruct{}, nil
}

func (ch *Handler) Steal(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Steal request, parameters %v", param)
	// TODO
	return "{Steal}", nil
}

func (ch *Handler) MonitorCond(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("MonitorCond request, parameters %v", param)
	if err := ch.monitor(param, ovsjson.Update2); err != nil {
		return nil, err
	}
	return ch.getMonitoredData(param.DatabaseName, param.MonitorCondRequests, false)
}

func (ch *Handler) MonitorCondChange(ctx context.Context, param []interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCondChange request, parameters %v", param)

	return "{Monitor_cond_change}", nil
}

func (ch *Handler) MonitorCondSince(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("MonitorCondSince request, parameters %v", param)
	if err := ch.monitor(param, ovsjson.Update3); err != nil {
		return nil, err
	}
	data, err := ch.getMonitoredData(param.DatabaseName, param.MonitorCondRequests, false)
	if err != nil {
		return nil, err
	}
	return []interface{}{false, ovsjson.ZERO_UUID, data}, nil
}

func (ch *Handler) SetDbChangeAware(ctx context.Context, param interface{}) interface{} {
	klog.V(5).Infof("SetDbChangeAware request, parameters %v", param)
	return ovsjson.EmptyStruct{}
}

func NewHandler(tctx context.Context, db Databaser, cli *clientv3.Client) *Handler {
	return &Handler{
		handlerContext: tctx, db: db, databaseLocks: map[string]Locker{}, monitors: map[interface{}]handlerMonitorData{},
		etcdClient: cli,
	}
}

func (ch *Handler) Cleanup() error {
	klog.Info("CLEAN UP do something")
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for _, m := range ch.databaseLocks {
		m.unlock()
	}
	for jsonValue, monitorHandler := range ch.monitors {
		ch.db.RemoveMonitors(monitorHandler.dataBaseName, monitorHandler.updaters, handlerKey{handler: ch, jsonValue: jsonValue})
	}
	return nil
}

func (ch *Handler) SetConnection(con ClientConnection) {
	ch.connection = con
}

func (ch *Handler) notify(jsonValue interface{}, updates ovsjson.TableUpdates) {
	klog.V(5).Infof("Monitor notification jsonValue %v", jsonValue)
	var err error
	handler, ok := ch.monitors[jsonValue]
	if !ok {
		klog.Errorf("Unknown jsonValue %s", jsonValue)
		return
	}
	switch handler.notificationType {
	case ovsjson.Update:
		err = ch.connection.Notify(ch.handlerContext, "update", []interface{}{jsonValue, updates})
	case ovsjson.Update2:
		err = ch.connection.Notify(ch.handlerContext, "update2", []interface{}{jsonValue, updates})
	case ovsjson.Update3:
		err = ch.connection.Notify(ch.handlerContext, "update3", []interface{}{jsonValue, ovsjson.ZERO_UUID, updates})
	}
	if err != nil {
		// TODO should we do something else
		klog.Error(err)
	}
}

func (ch *Handler) monitorCanceledNotification(jsonValue interface{}) {
	klog.V(5).Infof("monitorCanceledNotification %v", jsonValue)
	err := ch.connection.Notify(ch.handlerContext, "monitor_canceled", jsonValue)
	if err != nil {
		// TODO should we do something else
		klog.Error(err)
	}
}

func (ch *Handler) monitor(param ovsjson.CondMonitorParameters, notificationType ovsjson.UpdateNotificationType) error {
	if len(param.DatabaseName) == 0 {
		return fmt.Errorf("DataBase name is not specified")
	}
	jsonValue := param.JsonValue
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if _, ok := ch.monitors[jsonValue]; ok {
		return fmt.Errorf("duplicate json-value")
	}
	updatersMap := map[string][]*updater{}
	updaterKeys := map[string][]string{}

	for tableName, mcrs := range param.MonitorCondRequests {
		updaters := []*updater{}
		keys := []string{}
		for _, mcr := range mcrs {
			updater := mcrToUpdater(mcr, notificationType == ovsjson.Update)
			keys = append(keys, updater.key)
			updaters = append(updaters, updater)
		}
		updatersMap[tableName] = updaters
		updaterKeys[tableName] = keys
	}
	ch.monitors[jsonValue] = handlerMonitorData{dataBaseName: param.DatabaseName, notificationType: notificationType, updaters: updaterKeys}
	ch.db.AddMonitors(param.DatabaseName, updatersMap, handlerKey{jsonValue: jsonValue, handler: ch})
	return nil
}

func (ch *Handler) getMonitoredData(dataBase string, conditions map[string][]ovsjson.MonitorCondRequest, isV1 bool) (ovsjson.TableUpdates, error) {

	returnData := ovsjson.TableUpdates{}
	for tableName, mcrs := range conditions {
		if len(mcrs) > 1 {
			// TODO deal with the array
			klog.Warningf("MCR is not a singe %v", mcrs)
		}
		if mcrs[0].Select != nil && !libovsdb.MSIsTrue(mcrs[0].Select.Initial) {
			continue
		}
		resp, err := ch.db.GetData(common.NewTableKey(dataBase, tableName), false)
		if err != nil {
			return nil, err
		}
		d1 := ovsjson.TableUpdate{}
		for _, v := range resp.Kvs {
			data := map[string]interface{}{}
			json.Unmarshal(v.Value, &data)
			uuidSet, ok := data["uuid"]
			if !ok {
				err := fmt.Errorf("key %s, wrong formatting, doesn't include UUID %v", v.Key, data)
				klog.Error(err)
				return nil, err
			}
			uuid := uuidSet.([]interface{})[1]
			if len(mcrs[0].Columns) == 0 {
				delete(data, "uuid")
				delete(data, "_version")
			} else {
				columnsMap := common.StringArrayToMap(mcrs[0].Columns)
				for column := range data {
					if _, ok := columnsMap[column]; !ok {
						delete(data, column)
					}
				}
			}
			if isV1 {
				d1[uuid.(string)] = ovsjson.RowUpdate{New: &data}
			} else {
				d1[uuid.(string)] = ovsjson.RowUpdate{Initial: &data}
			}
		}
		returnData[tableName] = d1
	}
	return returnData, nil
}

/*
func (ch *Handler) newWatcher(prefix string) *watcher {

	ctx := context.Background()
	ctxt, cancel := context.WithCancel(ctx)
	wch := ch.db.Watch(ctxt, prefix)

	monitor := &watcher{cancel: cancel}
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
	}() // todo start
	return monitor
}
*/
