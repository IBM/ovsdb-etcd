package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/client/v3/concurrency"
	"sync"

	"github.com/ebay/libovsdb"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

type ClientConnection interface {
	Wait() error
	Stop()
	Notify(ctx context.Context, method string, params interface{}) error
}

type Handler struct {
	db Databaser

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
	transResponse := libovsdb.TransactResponse{}
	aborting := false
	dbname := "mydbname" // FIXME: this is the correct value param[0].(string)
	param = param[1 : len(param)-1]
	for i, v := range param {
		klog.V(6).Infof("Transact i=%d v=%v\n", i, v)
		opErr := fmt.Errorf("aborting: did not run operation")
		opResult := &libovsdb.OperationResult{}

		if !aborting {
			m, ok := v.(map[string]interface{})
			if !ok {
				continue
			}

			op := &libovsdb.Operation{}
			b, _ := json.Marshal(m)    // FIXME: handle error
			_ = json.Unmarshal(b, &op) // FIXME: handle error

			doOp := doOperation{
				dbname: dbname,
				db:     ch.db,
			}

			switch op.Op { // FIXME: handle error
			case "insert":
				opResult, opErr = doOp.Insert(op)
			case "select":
				opResult, opErr = doOp.Select(op)
			case "update":
				opResult, opErr = doOp.Update(op)
			case "mutate":
				opResult, opErr = doOp.Mutate(op)
			case "delete":
				opResult, opErr = doOp.Delete(op)
			case "wait":
				opResult, opErr = doOp.Wait(op)
			case "commit":
				opResult, opErr = doOp.Commit(op)
			case "abort":
				opResult, opErr = doOp.Abort(op)
			case "comment":
				opResult, opErr = doOp.Comment(op)
			case "assert":
				opResult, opErr = doOp.Assert(op)
			default:
				opErr = fmt.Errorf("bad operation: %s", op.Op)
				aborting = true
			}
		}
		transResponse.Result = append(transResponse.Result, *opResult)
		if opErr != nil {
			transResponse.Error = "aborting transaction: " + opErr.Error()
			aborting = true
		}
	}

	b, _ := json.Marshal(transResponse) // FIXME: handle error
	respMap := map[string]interface{}{}
	_ = json.Unmarshal(b, &respMap) // FIXME: handle error
	return respMap, nil
}

func (ch *Handler) Cancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Cancel request, parameters %v", param)

	return "{Cancel}", nil
}

func (ch *Handler) Monitor(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("Monitor request, parameters %v", param)
	for tableName, mcr := range param.MonitorCondRequests {
		// TODO handle Where, if Where contains uuid, it can be a part of the key
		key := fmt.Sprintf("%s/%s", param.DatabaseName, tableName)
		ch.db.AddMonitor(key, mcr[0], true, &handlerKey{ch, param.JsonValue})
	}
	return ch.getMonitoredData(param.DatabaseName, param.MonitorCondRequests, false)
}

func (ch *Handler) MonitorCancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCancel request, parameters %v", param)

	return "{Monitor_cancel}", nil
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

	return ch.getMonitoredData(param.DatabaseName, param.MonitorCondRequests, true)
}

func (ch *Handler) MonitorCondChange(ctx context.Context, param []interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCondChange request, parameters %v", param)

	return "{Monitor_cond_change}", nil
}

func (ch *Handler) MonitorCondSince(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("MonitorCondSince request, parameters %v", param)

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

func NewHandler(tctx context.Context, db Databaser) *Handler {
	return &Handler{
		handlerContext: tctx, db: db, databaseLocks: map[string]Locker{},
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
	ctx := context.Background()
	nType := ch.jsonValueToUpdateNotificationType[jsonValue]
	switch nType {
	case ovsjson.Update:
		err = ch.connection.Notify(ctx, "update", []interface{}{jsonValue, updates})
	case ovsjson.Update2:
		err = ch.connection.Notify(ctx, "update2", []interface{}{jsonValue, updates})
	case ovsjson.Update3:
		err = ch.connection.Notify(ctx, "update3", []interface{}{jsonValue, ovsjson.ZERO_UUID, updates})
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
		if mcrs[0].Select != nil && !mcrs[0].Select.Initial {
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
