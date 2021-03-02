package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/creachadair/jrpc2"
	"strconv"
	"strings"
	"time"

	ovsjson "github.com/roytman/ovsdb-etcd/pkg/json"
)

type ServOVSDB struct {
	dbServer DBServer
}

type InitialData struct {
	Name      string `json:"name"`
	Model     string `json:"model"`
	Connected bool   `json:"connected"`
	Schema    string `json:"schema"`
	Leader    bool   `json:"leader"`
}

type Databases struct {
	Database map[string]ovsjson.Initial `json:"Database"`
}

type TransactionResponse struct {
	Rows []map[string]string
}

// This operation retrieves an array whose elements are the names of the
//  databases that can be accessed over this management protocol
//  connection.
// "params": []
// The response object contains the following members:
//  	"result": [<db-name>,...]
//   	"error": null
//   	"id": same "id" as request
func (s *ServOVSDB) List_dbs(ctx context.Context, param interface{}) ([]string, error) {
	// fmt.Printf("List_dbs param %T %v\n", param, param)
	// TODO remove hardcoded list of DBs
	return []string{"_Server", "OVN_Northbound"}, nil
}

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
func (s *ServOVSDB) Get_schema(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Get_schema prame %T %v\n", param, param)
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
	schema, ok := s.dbServer.schemas[schemaName]
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

// This method causes the database server to execute a series of operations in the specified order on a given database.
// "params": [<db-name>, <operation>*]
// The response object contains the following members:
//   	"result": [<object>*]
//   	"error": null
//   	"id": same "id" as request
// Regardless of whether errors occur in the database operations, the response is always a JSON-RPC response with null
// "error" and a "result" member that is an array with the same number of elements as "params".  Each element of the
// "result" array corresponds to the same element of the "params" array.
func (s *ServOVSDB) Transact(ctx context.Context, param []interface{}) (interface{}, error) {
	for k, v := range param {
		fmt.Printf("Transact k = %d v= %#v\n", k, v)
		valuesMap, ok := v.(map[string]interface{})
		if ok {
			for km, vm := range valuesMap {
				fmt.Printf("\t  k = %v v= %+v\n", km, vm)
			}
			if valuesMap["op"] == "select" {
				tabel, okt := valuesMap["table"]
				if !okt {
					return nil, fmt.Errorf("Table is not specified")
				}
				colomns, _ := valuesMap["columns"]
				fmt.Printf("Columns type %T\n", colomns)
				resp, err := s.dbServer.GetMarshaled("ovsdb/"+tabel.(string), colomns.([]interface{}))
				if err != nil {
					return nil, err
				}
				tr := TransactionResponse{Rows: *resp}
				return tr, nil
			}
		}
	}

	return "{Transact}", nil
}

func (s *ServOVSDB) Cancel(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Cancel %T, %+v\n", param, param)

	return "{Cancel}", nil
}

// The "monitor" request enables a client to replicate tables or subsets of tables within an OVSDB database by
// requesting notifications of changes to those tables and by receiving the complete initial state of a table or a
// subset of a table.
// "params": [<db-name>, <json-value>, <monitor-requests>]
// The response object has the following members:
//   "result": <table-updates>  If no tables' initial contents are requested, then "result" is an empty object
//   "error": null
//   "id": same "id" as request
func (s *ServOVSDB) Monitor(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Monitor %T, %+v\n", param, param)

	return ovsjson.EmptyStruct{}, nil
}

func (s *ServOVSDB) Update(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Update %T, %+v\n", param, param)

	return "{Update}", nil
}

func (s *ServOVSDB) Monitor_cancel(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Monitor_cancel %T, %+v\n", param, param)

	return "{Monitor_cancel}", nil
}

// The database server supports an arbitrary number of locks, each of which is identified by a client-defined ID.
// At any given time, each lock may have at most one owner.
// The database will assign the client ownership of the lock as soon as it becomes available.  When multiple clients
// request the same lock, they will receive it in first-come, first-served order. The request completes and sends
// a response quickly, without waiting. The "locked" and "stolen" notifications report asynchronous changes to ownership.
// "params": [<id>]
// Returns "result": {"locked": boolean}
func (s *ServOVSDB) Lock(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Lock %T, %+v\n", param, param)
	defer notification(ctx)
	var id string
	// param is []interface{}, but just in case ...
	switch param.(type) {
	case []interface{}:
		intArray := param.([]interface{})
		if len(intArray) == 0 {
			// Error
			fmt.Printf("Empty params")
			return []interface{}{"locked", false}, nil
		} else {
			id = fmt.Sprintf("%s", intArray[0])
		}
	case string:
		id = param.(string)
	case interface{}:
		id = fmt.Sprintf("%s", param)
	}
	locked, err := s.dbServer.Lock(ctx context.Context, id)
	if err != nil {
		// TODO should we return error ?
		fmt.Printf("Lock returned error %v\n", err)
	}
	return []interface{}{"locked", locked}, nil
}

func notification(ctx context.Context) {
	go func() {
		for {
			time.Sleep(3 * time.Second)
			if err := jrpc2.PushNotify(ctx, "pushback", []string{"hello, friend"}); err != nil {
				fmt.Printf("notification %v\n", err)
				return

			}
		}
	}()
}

func (s *ServOVSDB) Unlock(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Unlock %T, %+v\n", param, param)
	return "{Unlock}", nil
}

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
func (s *ServOVSDB) Monitor_cond(ctx context.Context, param []interface{}) (interface{}, error) {
	fmt.Printf("Monitor_cond %T %+v\n", param, param)

	resp, err := s.dbServer.GetData("ovsdb/" + param[0].(string))
	if err != nil {
		return nil, err
	}

	databases := Databases{Database: map[string]ovsjson.Initial{}}
	for _, v := range resp.Kvs {
		keys := strings.Split(string(v.Key), "/")
		in, ok := databases.Database[keys[3]]
		var id InitialData
		if !ok {
			in = ovsjson.Initial{}
			databases.Database[keys[3]] = in
		}
		if in.Initial == nil {
			in.Initial = InitialData{}
		}
		id = in.Initial.(InitialData)
		switch keys[5] {
		case "name":
			id.Name = string(v.Value)
		case "connected":
			id.Connected, _ = strconv.ParseBool(string(v.Value))
		case "leader":
			id.Leader, _ = strconv.ParseBool(string(v.Value))
		case "model":
			id.Model = string(v.Value)
		case "schema":
			id.Schema = string(v.Value)
		}
		databases.Database[keys[3]] = ovsjson.Initial{Initial: id}
	}
	return databases, nil
}

func (s *ServOVSDB) Monitor_cond_change(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Monitor_cond_change %T, %+v\n", param, param)

	return "{Monitor_cond_change}", nil
}

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
func (s *ServOVSDB) Monitor_cond_since(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Monitor_cond_since %T, %+v\n", param, param)

	// TODO implement
	return []interface{}{false, ovsjson.ZERO_UUID, ovsjson.EmptyStruct{}}, nil
}

// A new RPC method added in Open vSwitch version 2.7.
// "params": null
// "result": "<server_id>"
// <server_id> is JSON string that contains a UUID that uniquely identifies the running OVSDB server process.
// A fresh UUID is generated when the process restarts.
func (s *ServOVSDB) Get_server_id(ctx context.Context, param interface{}) string {
	fmt.Printf("Get_server_id %+v\n", param)
	return s.dbServer.uuid
}

// RFC 7047 does not provide a way for a client to find out about some kinds of configuration changes, such as
// about databases added or removed while a client is connected to the server, or databases changing between read/write
// and read-only due to a transition between active and backup roles. Traditionally, ovsdb-server disconnects all of
// its clients when this happens, because this prompts a well-written client to reassess what is available from the
// server when it reconnects.
// By itself, this does not suppress ovsdb-server disconnection behavior, because a client might monitor this database
// without understanding its special semantics. Instead, ovsdb-server provides a special request: <Set_db_change_aware>
//
// "params": [<boolean>]
// If the boolean in the request is true, it suppresses the connection-closing behavior for the current connection,
// and false restores the default behavior. The reply is always the same:
// "result": {}
func (s *ServOVSDB) Set_db_change_aware(ctx context.Context, param interface{}) interface{} {
	fmt.Printf("Set_db_change_aware %+v\n", param)
	return ovsjson.EmptyStruct{}
}

func (s *ServOVSDB) Convert(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Convert %+v\n", param)
	return "{Convert}", nil
}

// The "echo" method can be used by both clients and servers to verify the liveness of a database connection.
// "params": JSON array with any contents
// The response object has the following members:
//     	"result": same as "params"
//     	"error": null
//		"id": the request "id" member
func (s *ServOVSDB) Echo(ctx context.Context, param interface{}) interface{}{
	fmt.Printf("Echo param %T %v\n", param, param)
	return param
}

func NewService(dbServer *DBServer) *ServOVSDB {
	return &ServOVSDB{dbServer: *dbServer}
}
