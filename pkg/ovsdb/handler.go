package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

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
	db         Databaser
	etcdClient *clientv3.Client

	jrpcServer     JrpcServer
	handlerContext context.Context
	clientCon      net.Conn

	mu sync.Mutex

	// jsonValueStr -> handlerMonitorData
	monitors      map[string]handlerMonitorData
	databaseLocks map[string]Locker
}

func (ch *Handler) Transact(ctx context.Context, params []interface{}) (interface{}, error) {
	klog.V(5).Infof("Transact request from %v, params %v", ch.clientCon.RemoteAddr(), params)
	klog.Flush()
	req, err := libovsdb.NewTransact(params)
	if err != nil {
		return nil, err
	}
	txn := NewTransaction(ch.etcdClient, req)
	txn.schemas = ch.db.GetSchemas()
	txn.Commit()
	klog.V(5).Infof("Transact response to %v: %s", ch.clientCon.RemoteAddr(), txn.response)
	return txn.response.Result, nil
}

func (ch *Handler) Cancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Cancel request from %v, param %v", ch.clientCon.RemoteAddr(), param)

	return "{Cancel}", nil
}

func (ch *Handler) Monitor(ctx context.Context, params []interface{}) (interface{}, error) {
	klog.V(5).Infof("Monitor request from %v, params %v", ch.clientCon.RemoteAddr(), params)
	updatersMap, err := ch.monitor(params, ovsjson.Update)
	if err != nil {
		klog.Errorf("Monitor from %v, params %v got an error: %s", ch.clientCon.RemoteAddr(), err)
		return nil, err
	}
	data, err := ch.getMonitoredData(updatersMap, true)
	klog.V(5).Infof("Monitor response to %v, params %v, err %v", ch.clientCon.RemoteAddr(), params, err)
	if err != nil {
		ch.removeMonitor(params[1])
		return nil, err
	}
	return data, nil
}

func (ch *Handler) MonitorCancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCancel request from %v, param %v", ch.clientCon.RemoteAddr(), param)
	err := ch.removeMonitor(param)
	if err != nil {
		return nil, err
	}
	return "{}", nil
}

func (ch *Handler) Lock(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Lock request from %v, param %v", ch.clientCon.RemoteAddr(), param)
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
			if err := ch.jrpcServer.Notify(ch.handlerContext, "locked", []string{id}); err != nil {
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
	klog.V(5).Infof("Unlock request from %v, param %v", ch.clientCon.RemoteAddr(), param)
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
	klog.V(5).Infof("Steal request from %v, param %v", ch.clientCon.RemoteAddr(), param)
	// TODO
	return "{Steal}", nil
}

func (ch *Handler) MonitorCond(ctx context.Context, params []interface{}) (interface{}, error) {
	klog.V(1).Infof("MonitorCond request from %v, param %v", ch.clientCon.RemoteAddr(), params)
	updatersMap, err := ch.monitor(params, ovsjson.Update2)
	if err != nil {
		klog.Errorf("MonitorCond from remote %v got an error: %s", ch.clientCon.RemoteAddr(), err)
		return nil, err
	}
	data, err := ch.getMonitoredData(updatersMap, false)
	klog.V(5).Infof("MonitorCond response to %v, params %v, err %v", ch.clientCon.RemoteAddr(), params, err)
	if err != nil {
		ch.removeMonitor(params[1])
		return nil, err
	}
	return data, nil
}

func (ch *Handler) MonitorCondChange(ctx context.Context, params []interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCondChange request from %v, params %v", ch.clientCon.RemoteAddr(), params)
	// TODO implement
	return "{Monitor_cond_change}", nil
}

func (ch *Handler) MonitorCondSince(ctx context.Context, params []interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCondSince request from %v, parameters %v", ch.clientCon.RemoteAddr(), params)
	updatersMap, err := ch.monitor(params, ovsjson.Update3)
	if err != nil {
		klog.Errorf("MonitorCondSince from remote %v got an error: %s", ch.clientCon.RemoteAddr(), err)
		return nil, err
	}
	data, err := ch.getMonitoredData(updatersMap, false)
	klog.V(5).Infof("MonitorCondSince response to %v, params %v, err %v", ch.clientCon.RemoteAddr(), params, err)
	if err != nil {
		ch.removeMonitor(params[1])
		return nil, err
	}
	return []interface{}{false, ovsjson.ZERO_UUID, data}, nil
}

func (ch *Handler) SetDbChangeAware(ctx context.Context, param interface{}) interface{} {
	klog.V(5).Infof("SetDbChangeAware request from %v, param %v", ch.clientCon.RemoteAddr(), param)
	return ovsjson.EmptyStruct{}
}

func NewHandler(tctx context.Context, db Databaser, cli *clientv3.Client) *Handler {
	return &Handler{
		handlerContext: tctx, db: db, databaseLocks: map[string]Locker{}, monitors: map[string]handlerMonitorData{},
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
	for jsonValueStr, monitorHandler := range ch.monitors {
		ch.db.RemoveMonitors(monitorHandler.dataBaseName, monitorHandler.updaters, handlerKey{handler: ch, jsonValueStr: jsonValueStr})
	}
	return nil
}

func (ch *Handler) SetConnection(jrpcSerer JrpcServer, clientCon net.Conn) {
	ch.jrpcServer = jrpcSerer
	ch.clientCon = clientCon
}

func (ch *Handler) notify(jsonValueStr string, updates ovsjson.TableUpdates) {
	handler, ok := ch.monitors[jsonValueStr]
	if !ok {
		klog.Errorf("Unknown jsonValue %s", jsonValueStr)
		return
	}
	if klog.V(7).Enabled() {
		klog.V(7).Infof("Monitor notification jsonValue %v to %v: %s", handler.jsonValue, ch.clientCon.RemoteAddr(), updates)
	} else {
		klog.V(5).Infof("Monitor notification jsonValue %v to %v", handler.jsonValue, ch.clientCon.RemoteAddr())
	}
	var err error
	switch handler.notificationType {
	case ovsjson.Update:
		err = ch.jrpcServer.Notify(ch.handlerContext, "update", []interface{}{handler.jsonValue, updates})
	case ovsjson.Update2:
		err = ch.jrpcServer.Notify(ch.handlerContext, "update2", []interface{}{handler.jsonValue, updates})
	case ovsjson.Update3:
		err = ch.jrpcServer.Notify(ch.handlerContext, "update3", []interface{}{handler.jsonValue, ovsjson.ZERO_UUID, updates})
	}
	if err != nil {
		// TODO should we do something else
		klog.Error(err)
	}
}

func (ch *Handler) monitorCanceledNotification(jsonValue interface{}) {
	klog.V(5).Infof("monitorCanceledNotification %v", jsonValue)
	err := ch.jrpcServer.Notify(ch.handlerContext, "monitor_canceled", jsonValue)
	if err != nil {
		// TODO should we do something else
		klog.Error(err)
	}
}

func (ch *Handler) removeMonitor(jsonValue interface{}) error {
	jsonStr := ovsjson.InterfaceToString(jsonValue)
	klog.V(5).Infof("removeMonitor %v", jsonStr)
	buf, err := json.Marshal(jsonValue)
	if err != nil {
		klog.Errorf("removeMonitor %v", err)
		return err
	}
	var tmp json.RawMessage
	if err := json.Unmarshal(buf, &tmp); err != nil {
		klog.Errorf("removeMonitor unmarshal %v", err)
		return err
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	monitorHandler, ok := ch.monitors[ovsjson.InterfaceToString(tmp)]
	if !ok {
		klog.V(5).Infof("removeMonitor unknown monitor")
		return fmt.Errorf("unknown monitor")
	}
	ch.db.RemoveMonitors(monitorHandler.dataBaseName, monitorHandler.updaters, handlerKey{handler: ch, jsonValueStr: jsonStr})
	delete(ch.monitors, jsonStr)
	return nil
}

func (ch *Handler) monitor(params []interface{}, notificationType ovsjson.UpdateNotificationType) (Key2Updaters, error) {
	buf, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	cmpr := ovsjson.CondMonitorParameters{}
	err = json.Unmarshal(buf, &cmpr)
	if err != nil {
		return nil, err
	}
	if len(cmpr.DatabaseName) == 0 {
		return nil, fmt.Errorf("monitored dataBase name is empty")
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	jsonStr := ovsjson.InterfaceToString(cmpr.JsonValue)
	if _, ok := ch.monitors[jsonStr]; ok {
		return nil, fmt.Errorf("duplicate json-value")
	}
	updatersMap := Key2Updaters{}
	updaterKeys := map[string][]string{}

	for tableName, mcrs := range cmpr.MonitorCondRequests {
		updaters := []updater{}
		keys := []string{}
		for _, mcr := range mcrs {
			updater := mcrToUpdater(mcr, notificationType == ovsjson.Update)
			keys = append(keys, updater.key)
			updaters = append(updaters, *updater)
		}
		updatersMap[common.NewTableKey(cmpr.DatabaseName, tableName)] = updaters
		updaterKeys[tableName] = keys
	}
	ch.monitors[jsonStr] = handlerMonitorData{
		dataBaseName:     cmpr.DatabaseName,
		notificationType: notificationType,
		updaters:         updaterKeys,
		jsonValue:        cmpr.JsonValue}
	ch.db.AddMonitors(cmpr.DatabaseName, updatersMap, handlerKey{jsonValueStr: jsonStr, handler: ch})
	return updatersMap, nil
}

func (ch *Handler) getMonitoredData(updatersMap Key2Updaters, isV1 bool) (ovsjson.TableUpdates, error) {
	returnData := ovsjson.TableUpdates{}
	for tableKey, updaters := range updatersMap {
		if len(updaters) == 0 {
			// nothing to update
			continue
		}
		// validate that Initial is required
		reqInitial := false
		for _, updater := range updaters {
			reqInitial := reqInitial || libovsdb.MSIsTrue(updater.Select.Initial)
			if reqInitial {
				break
			}
		}
		resp, err := ch.db.GetData(tableKey, false)
		if err != nil {
			return nil, err
		}
		d1 := ovsjson.TableUpdate{}
		for _, kv := range resp.Kvs {
			for _, updater := range updaters {
				row, uuid, err := updater.prepareCreateRowInitial(&kv.Value)
				if err != nil {
					klog.Errorf("prepareCreateRowInitial returned %s", err)
					return nil, err
				}
				klog.V(8).Infof("processing getMonitoredData %v  row %v", d1, row)
				// TODO merge
				if row != nil {
					d1[uuid] = *row
				} else {
					klog.Info("row is nil")
				}
			}
		}
		if len(d1) > 0 {
			returnData[tableKey.TableName] = d1
		}
	}
	klog.V(6).Infof("getMonitoredData: %v", returnData)
	return returnData, nil
}
