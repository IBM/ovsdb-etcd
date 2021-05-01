package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/ibm/ovsdb-etcd/pkg/common"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

// The interface that provides OVSDB jrpc methods defined by RFC 7047 and later extended by the OVN/OVS community,
// see https://tools.ietf.org/html/rfc7047 and https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7
type Servicer interface {

	// RFC 7047 section 4.1.1
	// This operation retrieves an array whose elements are the names of the
	//  databases that can be accessed over this management protocol
	//  connection.
	// "params": []
	// The response object contains the following members:
	//  	"result": [<db-name>,...]
	//   	"error": null
	//   	"id": same "id" as request
	ListDbs(ctx context.Context, param interface{}) ([]string, error)

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
	Monitor(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error)

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

const (
	INT_SERVER    = "_Server"
	INT_DATABASES = "Database"
)

type Service struct {
	db   Databaser
	uuid string
}

func (s *Service) ListDbs(ctx context.Context, param interface{}) ([]string, error) {
	klog.V(5).Infof("ListDbs request")
	resp, err := s.db.GetData(common.NewTableKey(INT_SERVER, INT_DATABASES), true)
	if err != nil {
		return nil, err
	}
	dbs := []string{}
	for _, kv := range resp.Kvs {
		key, err := common.ParseKey(string(kv.Key))
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, key.UUID)
	}
	return dbs, nil
}

func (s *Service) GetSchema(ctx context.Context, param interface{}) (interface{}, error) {
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
	schema, ok := s.db.GetSchema(schemaName)
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

func (s *Service) GetServerId(ctx context.Context) string {
	klog.V(5).Infof("GetServerId request")
	return s.uuid
}

func (s *Service) Convert(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Convert request, parameters %v", param)
	return "{Convert}", nil
}

func (s *Service) Echo(ctx context.Context, param interface{}) interface{} {
	klog.V(5).Infof("Echo request, parameters %v", param)
	return param
}

func NewService(db Databaser) *Service {
	return &Service{
		db:   db,
		uuid: uuid.NewString(),
	}
}
