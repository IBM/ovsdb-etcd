package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/ebay/libovsdb"
	"k8s.io/klog/v2"

	ovsjson "github.com/ibm/ovsdb-etcd/pkg/json"
)

// The interface that provides OVSDB jrpc methods defined by RFC 7047 and later extended by the OVN/OVS community,
// see https://tools.ietf.org/html/rfc7047 and https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7
type OVSDB interface {

	// RFC 7047 section 4.1.1
	// This operation retrieves an array whose elements are the names of the
	//  databases that can be accessed over this management protocol
	//  connection.
	// "params": []
	// The response object contains the following members:
	//  	"result": [<db-name>,...]
	//   	"error": null
	//   	"id": same "id" as request
	ListDbs(ctx context.Context) ([]string, error)

	// RFC 7047 section 4.1.2
	// This operation retrieves a <database-schema> that describes hosted database <db-name>.
	// "params": [<db-name>]
	// The response object contains the following members:
	// 		"result": <database-schema>
	//   	"error": null
	//   	"id": same "id" as request
	// In the event that the database named in the request does not exist, the server sends a JSON-RPC error response
	// of the following form:
	// 		"result": null
	//      "error": "unknown database"
	//      "id": same "id" as request
	GetSchema(ctx context.Context, param interface{}) (interface{}, error)

	// RFC 7047 section 4.1.3
	// This method causes the database server to execute a series of operations in the specified order on a given database.
	// "params": [<db-name>, <operation>*]
	// The response object contains the following members:
	//   	"result": [<object>*]
	//   	"error": null
	//   	"id": same "id" as request
	// Regardless of whether errors occur in the database operations, the response is always a JSON-RPC response with null
	// "error" and a "result" member that is an array with the same number of elements as "params".  Each element of the
	// "result" array corresponds to the same element of the "params" array.
	Transact(ctx context.Context, param []interface{}) (interface{}, error)

	// RFC 7047 section 4.1.4
	// The "cancel" method is a JSON-RPC notification, i.e., no matching response is provided.
	//	It instructs the database server to  immediately complete or cancel the "transact" request whose "id" is
	//  the same as the notification's "params" value.
	// "params": [the "id" for an outstanding request]
	// The "cancel" notification itself has no reply.
	Cancel(ctx context.Context, param interface{}) (interface{}, error)

	// RFC 7047 section 4.1.5
	// The "monitor" request enables a client to replicate tables or subsets of tables within an OVSDB database by
	// requesting notifications of changes to those tables and by receiving the complete initial state of a table or a
	// subset of a table.
	// "params": [<db-name>, <json-value>, <monitor-requests>]
	// The response object has the following members:
	//   "result": <table-updates>  If no tables' initial contents are requested, then "result" is an empty object
	//   "error": null
	//   "id": same "id" as request
	Monitor(ctx context.Context, param interface{}) (interface{}, error)

	// RFC 7047 section 4.1.7
	// The "monitor_cancel" request cancels a previously issued monitor request.
	// "params": [<json-value>] matches the <json-value> for the ongoing "monitor" request that is to be canceled.
	// The response to this request has the following members:
	//   "result": {}
	//   "error": null
	// If a monitor cancellation request refers to an unknown monitor request, an error response with the following
	// members is returned:
	//   "result": null
	//   "error": "unknown monitor"
	MonitorCancel(ctx context.Context, param interface{}) (interface{}, error)

	// RFC 7047 section 4.1.8
	// The database server supports an arbitrary number of locks, each of which is identified by a client-defined ID.
	// At any given time, each lock may have at most one owner.
	// The database will assign the client ownership of the lock as soon as it becomes available.  When multiple clients
	// request the same lock, they will receive it in first-come, first-served order. The request completes and sends
	// a response quickly, without waiting. The "locked" and "stolen" notifications report asynchronous changes to ownership.
	// "params": [<id>]
	// Returns: "result": {"locked": boolean}
	Lock(ctx context.Context, param interface{}) (interface{}, error)

	// RFC 7047 section 4.1.8
	// If the client owns the lock, this operation releases it. If the client has requested ownership of the lock,
	// this cancels the request.
	// "params": [<id>]
	// Returns: "result": {}
	Unlock(ctx context.Context, param interface{}) (interface{}, error)

	// RFC 7047 section 4.1.8
	// The database immediately assigns this client ownership of the lock.  If there is an existing owner, it loses
	// ownership.
	// "params": [<id>]
	// Returns: "result": {"locked": true}
	Steal(ctx context.Context, param interface{}) (interface{}, error)

	// ovsdb-server.7 section 4.1.12
	// The monitor_cond request enables a client to replicate subsets of tables within an OVSDB database by requesting
	// notifications of changes to rows matching one of the conditions specified in where by receiving the specified
	// contents of these rows when table updates occur. monitor_cond also allows a more efficient update notifications
	// by receiving <table-updates2> notifications
	//
	// "params": [<db-name>, <json-value>, <monitor-cond-requests>]
	// The <json-value> parameter is used to match subsequent update notifications
	//  The <monitor-cond-requests> object maps the name of the table to an array of <monitor-cond-request>.
	//
	// Each <monitor-cond-request> is an object with the following members:
	// 		"columns": [<column>*]            optional
	//		"where": [<condition>*]           optional
	//		"select": <monitor-select>        optional
	//
	// The columns, if present, define the columns within the table to be monitored that match conditions.
	// If not present, all columns are monitored.
	//
	// The where, if present, is a JSON array of <condition> and boolean values. If not present or condition is an empty
	// array, implicit True will be considered and updates on all rows will be sent.
	//
	//  <monitor-select> is an object with the following members:
	//		"initial": <boolean>              optional
	//		"insert": <boolean>               optional
	//		"delete": <boolean>               optional
	//		"modify": <boolean>               optional
	//
	// The response object has the following members:
	//  "result": <table-updates2>
	//  "error": null
	//  "id": same "id" as request
	MonitorCond(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error)

	// ovsdb-server.7 section 4.1.13
	// enables a client to change an existing monitor_cond replication of the database by specifying a new condition
	// and columns for each replicated table. Currently changing the columns set is not supported.
	// "params": [<json-value>, <json-value>, <monitor-cond-update-requests>]
	// Returns:
	// 		"result": null
	//		"error": null
	MonitorCondChange(ctx context.Context, param []interface{}) (interface{}, error)

	// ovsdb-server.7 section 4.1.15
	// Enables a client to request changes that happened after a specific transaction id. A client can use this feature
	// to request only latest changes after a server connection reset instead of re-transfer all data from the server again.
	//
	// "params": [<db-name>, <json-value>, <monitor-cond-requests>, <last-txn-id>]
	// The <json-value> parameter is used to match subsequent update notifications to this request.
	// The <monitor-cond-requests> object maps the name of the table to an array of <monitor-cond-request>. Each
	// <monitor-cond-request> is an object with the following members:
	//    	"columns": [<column>*]            optional
	//		"where": [<condition>*]           optional
	//		"select": <monitor-select>        optional
	// <monitor-select> is an object with the following members:
	// 		"initial": <boolean>              optional
	//		"insert": <boolean>               optional
	//		"delete": <boolean>               optional
	//		"modify": <boolean>               optional
	//
	// "result": [<found>, <last-txn-id>, <table-updates2>]
	// The <found> is a boolean value that tells if the <last-txn-id> requested by client is found in server’s history or
	// not. If true, the changes after that version up to current is sent. Otherwise, all data is sent.
	//  The <last-txn-id> is the transaction id that identifies the latest transaction included in the changes in
	//  <table-updates2> of this response, so that client can keep tracking. If there is no change involved in this
	// response, it is the same as the <last-txn-id> in the request if <found> is true, or zero uuid if <found> is false.
	// If the server does not support transaction uuid, it will be zero uuid as well.
	MonitorCondSince(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error)

	// ovsdb-server.7 section 4.1.17
	// Returns a UUID that uniquely identifies the running OVSDB server process
	// A fresh UUID is generated when the process restarts.
	// "params": null
	// Returns:
	//		"result": "<server_id>"
	// 		<server_id> is JSON string that contains a server process UUID
	GetServerId(ctx context.Context) string

	// ovsdb-server.7 section 4.1.18
	// RFC 7047 does not provide a way for a client to find out about some kinds of configuration changes, such as
	// about databases added or removed while a client is connected to the server, or databases changing between read/write
	// and read-only due to a transition between active and backup roles. Traditionally, ovsdb-server disconnects all of
	// its clients when this happens, because this prompts a well-written client to reassess what is available from the
	// server when it reconnects.
	// By itself, this does not suppress ovsdb-server disconnection behavior, because a client might monitor this database
	// without understanding its special semantics. Instead, ovsdb-server provides a special request: <Set_db_change_aware>
	//
	// 		"params": [<boolean>]
	// If the boolean in the request is true, it suppresses the connection-closing behavior for the current connection,
	// and false restores the default behavior. The reply is always the same:
	// "result": {}
	SetDbChangeAware(ctx context.Context, param interface{}) interface{}

	// ovsdb-server.7 section 4.1.19
	// Converts an online database from one schema to another. The request contains the following members:
	//
	// 		"params": [<db-name>, <database-schema>]
	Convert(ctx context.Context, param interface{}) (interface{}, error)

	// RFC 7047 section 4.1.11
	// Can be used by both clients and servers to verify the liveness of a database connection.
	// "params": JSON array with any contents
	// Returns : "result": same as "params"
	Echo(ctx context.Context, param interface{}) interface{}
}

type ServOVSDB struct {
	dbServer DBServerInterface
}

func (s *ServOVSDB) ListDbs(ctx context.Context) ([]string, error) {
	klog.V(5).Infof("ListDbs request")
	resp, err := s.dbServer.GetData("ovsdb/_Server/Database/", true)
	if err != nil {
		return nil, err
	}
	dbs := []string{}
	for _, kv := range resp.Kvs {
		slices := strings.Split(string(kv.Key), "/")
		dbs = append(dbs, slices[len(slices)-1])
	}
	return dbs, nil
}

func (s *ServOVSDB) GetSchema(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("GetSchema request, parameters %v", param)

	var schemaName string
	switch param.(type) {
	case string:
		schemaName = param.(string)
	case []string:
		schemaName = param.([]string)[0]
	case []interface{}:
		schemaName = fmt.Sprintf("%s", param.([]interface{})[0])
	default:
		// probably is a bad idea
		schemaName = fmt.Sprintf("%s", param)
	}
	schema, ok := s.dbServer.GetSchema(schemaName)
	if !ok {
		return nil, fmt.Errorf("unknown database")
	}
	var f interface{}
	err := json.Unmarshal([]byte(schema), &f)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *ServOVSDB) Transact(ctx context.Context, param []interface{}) (interface{}, error) {
	klog.V(5).Infof("Transact request, parameters %v", param)
	transResponse := libovsdb.TransactResponse{}
	aborting := false
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

			doOp := doOperation{dbServer: s.dbServer}

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

func (s *ServOVSDB) Cancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Cancel request, parameters %v", param)

	return "{Cancel}", nil
}

func (s *ServOVSDB) Monitor(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Monitor request, parameters %v", param)

	return ovsjson.EmptyStruct{}, nil
}

func (s *ServOVSDB) MonitorCancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCancel request, parameters %v", param)

	return "{Monitor_cancel}", nil
}

func (s *ServOVSDB) Lock(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Lock request, parameters %v", param)
	//defer notification(ctx)
	var id string
	// param is []interface{}, but just in case ...
	switch param.(type) {
	case []interface{}:
		intArray := param.([]interface{})
		if len(intArray) == 0 {
			// Error
			klog.Warningf("Empty params")
			return []interface{}{"locked", false}, nil
		} else {
			id = fmt.Sprintf("%s", intArray[0])
		}
	case string:
		id = param.(string)
	case interface{}:
		id = fmt.Sprintf("%s", param)
	}
	locked, err := s.dbServer.Lock(ctx, id)
	if err != nil {
		// TODO should we return error ?
		klog.Warningf("Lock returned error %v\n", err)
	}
	return []interface{}{"locked", locked}, nil
}

func notification(ctx context.Context) {
	go func() {
		for {
			time.Sleep(3 * time.Second)
			if err := jrpc2.PushNotify(ctx, "pushback", []string{"hello, friend"}); err != nil {
				klog.Errorf("notification %v\n", err)
				return

			}
		}
	}()
}

func (s *ServOVSDB) Unlock(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Unlock request, parameters %v", param)
	var id string
	// param is []interface{}, but just in case ...
	switch param.(type) {
	case []interface{}:
		intArray := param.([]interface{})
		if len(intArray) == 0 {
			// Error
			klog.Warningf("Empty params")
			return []interface{}{"locked", false}, nil
		} else {
			id = fmt.Sprintf("%s", intArray[0])
		}
	case string:
		id = param.(string)
	case interface{}:
		id = fmt.Sprintf("%s", param)
	}
	_ = s.dbServer.Unlock(ctx, id)
	return "{Unlock}", nil
}

func (s *ServOVSDB) Steal(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Steal request, parameters %v", param)
	// TODO
	return "{Steal}", nil
}

func (s *ServOVSDB) MonitorCond(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("MonitorCond request, parameters %v", param)

	return s.getMonitoredData(param.DatabaseName, param.MonitorCondRequests)
}

func (s *ServOVSDB) MonitorCondChange(ctx context.Context, param []interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCondChange request, parameters %v", param)

	return "{Monitor_cond_change}", nil
}

func (s *ServOVSDB) MonitorCondSince(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("MonitorCondSince request, parameters %v", param)

	data, err := s.getMonitoredData(param.DatabaseName, param.MonitorCondRequests)
	if err != nil {
		return nil, err
	}
	return []interface{}{false, ovsjson.ZERO_UUID, data}, nil
}

func (s *ServOVSDB) GetServerId(ctx context.Context) string {
	klog.V(5).Infof("GetServerId request")
	return s.dbServer.GetUUID()
}

func (s *ServOVSDB) SetDbChangeAware(ctx context.Context, param interface{}) interface{} {
	klog.V(5).Infof("SetDbChangeAware request, parameters %v", param)
	return ovsjson.EmptyStruct{}
}

func (s *ServOVSDB) Convert(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Convert request, parameters %v", param)
	return "{Convert}", nil
}

func (s *ServOVSDB) Echo(ctx context.Context, param interface{}) interface{} {
	klog.V(5).Infof("Echo request, parameters %v", param)
	return param
}

func NewService(dbServer DBServerInterface) *ServOVSDB {
	return &ServOVSDB{dbServer: dbServer}
}

// TODO move to utilities
func arrayToMap(input []interface{}) map[interface{}]bool {
	ret := map[interface{}]bool{}
	for _, str := range input {
		ret[str] = true
	}
	return ret
}

func (s *ServOVSDB) getMonitoredData(dataBase string, conditions map[string][]ovsjson.MonitorCondRequest) (map[string]map[string]ovsjson.RowUpdate2, error) {

	returnData := map[string]map[string]ovsjson.RowUpdate2{}
	for tableName, mcrs := range conditions {
		resp, err := s.dbServer.GetData("ovsdb/"+dataBase+"/"+tableName, false)
		if err != nil {
			return nil, err
		}
		if dataBase == "_Server" && tableName == "Database" {
			if len(mcrs) > 1 {
				klog.Warningf("MCR is not a singe %v", mcrs)
			}
			d1 := map[string]ovsjson.RowUpdate2{}
			for _, v := range resp.Kvs {
				data := map[string]interface{}{}
				json.Unmarshal(v.Value, &data)
				uuidSet := data["uuid"]
				uuid := uuidSet.([]interface{})[1]
				if len(mcrs[0].Columns) == 0 {
					delete(data, "uuid")
					delete(data, "version")
				} else {
					columnsMap := arrayToMap(mcrs[0].Columns)
					for column := range data {
						if _, ok := columnsMap[column]; !ok {
							delete(data, column)
						}
					}
				}
				d1[uuid.(string)] = ovsjson.Initial{Initial: data}
			}
			returnData[tableName] = d1
		} else {
			// TODO
		}
	}
	return returnData, nil
}
