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
	// map from jason-values to monitors
	jsonValueToMonitors               map[interface{}][]*monitor
	jsonValueToUpdateNotificationType map[interface{}]ovsjson.UpdateNotificationType

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
	return txn.response, nil
}

func (ch *Handler) Cancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Cancel request, parameters %v", param)

	return "{Cancel}", nil
}

func (ch *Handler) Monitor(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("Monitor request, parameters %v", param)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if err := ch.createOrDieMonitorsArray(param.JsonValue.(string)); err != nil {
		return nil, err
	}
	ch.jsonValueToUpdateNotificationType[param.JsonValue.(string)] = ovsjson.Update
	monitors := ch.jsonValueToMonitors[param.JsonValue]

	for tableName, mcrs := range param.MonitorCondRequests {
		// TODO handle Where, if Where contains uuid, it can be a part of the key
		key := fmt.Sprintf("%s/%s", param.DatabaseName, tableName)
		for _, mcr := range mcrs {
			ch.db.AddMonitor(key, mcr, true, &handlerKey{ch, param.JsonValue})
			// TODO add monitor
			monitors = append(monitors, nil)
		}
		// TODO check if we need it?
		ch.jsonValueToMonitors[param.JsonValue] = monitors
	}
	return ch.getMonitoredData(param.DatabaseName, param.MonitorCondRequests, false)
}

func (ch *Handler) MonitorCancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCancel request, parameters %v", param)
	jsonValue, err := common.ParamsToString(param)
	if err != nil {
		return nil, err
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	monitors, ok := ch.jsonValueToMonitors[jsonValue]
	if !ok {
		return nil, fmt.Errorf("unknown monitor")
	}
	for range monitors {
		// TODO
	}
	delete(ch.jsonValueToMonitors, jsonValue)
	delete(ch.jsonValueToUpdateNotificationType, jsonValue)
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
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if err := ch.createOrDieMonitorsArray(param.JsonValue.(string)); err != nil {
		return nil, err
	}
	ch.jsonValueToUpdateNotificationType[param.JsonValue.(string)] = ovsjson.Update2
	return ch.getMonitoredData(param.DatabaseName, param.MonitorCondRequests, true)
}

func (ch *Handler) MonitorCondChange(ctx context.Context, param []interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCondChange request, parameters %v", param)

	return "{Monitor_cond_change}", nil
}

func (ch *Handler) MonitorCondSince(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("MonitorCondSince request, parameters %v", param)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if err := ch.createOrDieMonitorsArray(param.JsonValue.(string)); err != nil {
		return nil, err
	}
	ch.jsonValueToUpdateNotificationType[param.JsonValue.(string)] = ovsjson.Update3
	data, err := ch.getMonitoredData(param.DatabaseName, param.MonitorCondRequests, true)
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
		handlerContext: tctx, db: db, databaseLocks: map[string]Locker{},
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
	return nil
}

func (ch *Handler) SetConnection(con ClientConnection) {
	ch.connection = con
}

func (ch *Handler) notify(jsonValue interface{}, updates ovsjson.TableUpdates) {
	var err error
	nType := ch.jsonValueToUpdateNotificationType[jsonValue]
	switch nType {
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

func (ch *Handler) getMonitoredData(dataBase string, conditions map[string][]ovsjson.MonitorCondRequest, isV2 bool) (ovsjson.TableUpdates, error) {

	returnData := ovsjson.TableUpdates{}
	for tableName, mcrs := range conditions {
		if len(mcrs) > 1 {
			// TODO deal with the array
			klog.Warningf("MCR is not a singe %v", mcrs)
		}
		if mcrs[0].Select != nil && !libovsdb.MSIsTrue(mcrs[0].Select.Initial) {
			continue
		}
		resp, err := ch.db.GetData(dataBase+"/"+tableName, false)
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
			if isV2 {
				d1[uuid.(string)] = ovsjson.RowUpdate{Initial: &data}
			} else {
				d1[uuid.(string)] = ovsjson.RowUpdate{New: &data}
			}
		}
		returnData[tableName] = d1
	}
	return returnData, nil
}

// called with ch.mu locked
func (ch *Handler) createOrDieMonitorsArray(jsonValue string) error {
	if _, ok := ch.jsonValueToMonitors[jsonValue]; ok {
		return fmt.Errorf("duplicate json-value")
	}
	ch.jsonValueToMonitors[jsonValue] = []*monitor{}
	return nil
}
