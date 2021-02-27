package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

	return  "{Cancel}", nil
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

	return  ovsjson.EmptyStruct{}, nil
}

func (s *ServOVSDB) Update(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Update %T, %+v\n", param, param)

	return "{Update}", nil
}

func (s *ServOVSDB) Monitor_cancel(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Monitor_cancel %T, %+v\n", param, param)

	return "{Monitor_cancel}", nil
}

func (s *ServOVSDB) Lock(ctx context.Context, param interface{}) (interface{}, error){
	fmt.Printf("Lock %T, %+v\n", param, param)

	return "{Lock}", nil
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

func (s *ServOVSDB) Monitor_cond_change(ctx context.Context, param interface{}) (interface{}, error){
	fmt.Printf("Monitor_cond_change %T, %+v\n", param, param)

	return "{Monitor_cond_change}", nil
}

func (s *ServOVSDB) Monitor_cond_since(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Monitor_cond_since %T, %+v\n", param, param)

	return  "{Monitor_cond_since}", nil
}

func (s *ServOVSDB) Get_server_id(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Get_server_id %+v\n", param)
	return "{Get_server_id}", nil
}

func (s *ServOVSDB) Set_db_change_aware(ctx context.Context, param interface{}) (interface{}, error){
	fmt.Printf("Set_db_change_aware %+v\n", param)
	return "{Set_db_change_aware}", nil
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
func (s *ServOVSDB) Echo(ctx context.Context, param interface{}) (interface{}, error) {
	fmt.Printf("Echo param %T %v\n", param, param)
	return param, nil
}

func NewService(dbServer *DBServer) *ServOVSDB {
	return &ServOVSDB{dbServer: *dbServer}
}
