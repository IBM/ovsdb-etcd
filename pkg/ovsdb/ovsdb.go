package ovsdb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type ServOVSDB struct {
	dbServer DBServer
}

type Initial struct {
	InitialData	`json:"initial"`
}

type InitialData struct {
	Name string 	`json:"name"`
	Model string 	`json:"model"`
	Connected bool	`json:"connected"`
	Schema string	`json:"schema"`
	Leader bool		`json:"leader"`
}

type Databases struct {
	Database map[string]Initial `json:"Database"`
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
func (s *ServOVSDB) List_dbs(param *interface{}, reply *interface{}) error {
	fmt.Println("List_dbs")
	// TODO remove hardcoded list of DBs
	*reply = []string{"_Server", "OVN_Northbound", "tests"}
	return nil
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
func (s *ServOVSDB) Get_schema(schemaName string, reply *interface{}) error {
	fmt.Printf("Get_schema %s\n", schemaName)
	schema, ok := s.dbServer.schemas[schemaName]
	if !ok {
		*reply = nil
		return fmt.Errorf("unknown database")
	}
	var f interface{}
	err := json.Unmarshal([]byte(schema), &f)
	if err != nil {
		return err
	}
	*reply = f
	return nil
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
func (s *ServOVSDB) Transact(param []interface{}, reply *interface{}) error {
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
					return fmt.Errorf("Table is not specified")
				}
				colomns, _ := valuesMap["columns"]
				fmt.Printf("Columns type %T\n", colomns)
				resp, err := s.dbServer.GetMarshaled("ovsdb/" + tabel.(string), colomns.([]interface{}))
				if err != nil {
					return err
				}
				tr := TransactionResponse{Rows: *resp}
				*reply = tr
				return nil
			}
		}
	}

	*reply = "{Transact}"
	return nil
}

func (s *ServOVSDB) Cancel(param *interface{}, reply *interface{}) error {
	fmt.Printf("Cancel %T, %+v\n", *param, *param)

	*reply = "{Cancel}"
	return nil
}

// The "monitor" request enables a client to replicate tables or subsets of tables within an OVSDB database by
// requesting notifications of changes to those tables and by receiving the complete initial state of a table or a
// subset of a table.
// "params": [<db-name>, <json-value>, <monitor-requests>]
// The response object has the following members:
//   "result": <table-updates>
//   "error": null
//   "id": same "id" as request
func (s *ServOVSDB) Monitor(param *interface{}, reply *interface{}) error {
	fmt.Printf("Monitor %T, %+v\n", *param, *param)

	*reply = "{Monitor}"
	return nil
}

func (s *ServOVSDB) Update(param *interface{}, reply *interface{}) error {
	fmt.Printf("Update %T, %+v\n", *param, *param)

	*reply = "{Update}"
	return nil
}

func (s *ServOVSDB) Monitor_cancel  (param *interface{}, reply *interface{}) error {
	fmt.Printf("Monitor_cancel %T, %+v\n", *param, *param)

	*reply = "{Monitor_cancel}"
	return nil
}

func (s *ServOVSDB) Lock  (param *interface{}, reply *interface{}) error {
	fmt.Printf("Lock %T, %+v\n", *param, *param)

	*reply = "{Lock}"
	return nil
}

func (s *ServOVSDB) Unlock  (param *interface{}, reply *interface{}) error {
	fmt.Printf("Unlock %T, %+v\n", *param, *param)

	*reply = "{Unlock}"
	return nil
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
func (s *ServOVSDB) Monitor_cond(param []interface{}, reply *interface{}) error {
	fmt.Printf("Monitor_cond %T %+v\n", param, param)

	resp, err := s.dbServer.GetData("ovsdb/" + param[0].(string))
	if err != nil {
		return err
	}
	databases := Databases{Database:map[string]Initial{}}
	for _, v := range resp.Kvs {
		keys := strings.Split(string(v.Key), "/")
		db, ok := databases.Database[keys[3]]
		if !ok {
			db  = Initial{}
		}
		switch keys[5] {
		case "name":
			db.Name = string(v.Value)
		case "connected" :
			db.Connected, _ = strconv.ParseBool(string(v.Value))
		case "leader" :
			db.Leader, _ = strconv.ParseBool(string(v.Value))
		case "model":
			db.Model = string(v.Value)
		case "schema":
			db.Schema = string(v.Value)
		}
		databases.Database[keys[3]] = db
	}
	*reply = databases
	return nil
}

func (s *ServOVSDB) Monitor_cond_change(param *interface{}, reply *interface{}) error {
	fmt.Printf("Monitor_cond_change %T, %+v\n", *param, *param)

	*reply = "{Monitor_cond_change}"
	return nil
}

func (s *ServOVSDB) Monitor_cond_since(param *interface{}, reply *interface{}) error {
	fmt.Printf("Monitor_cond_since %T, %+v\n", *param, *param)

	*reply = "{Monitor_cond_since}"
	return nil
}

func (s *ServOVSDB) Get_server_id(param *interface{}, reply *interface{}) error {
	fmt.Printf("Get_server_id %+v\n", *param)
	*reply = "{Get_server_id}"
	return nil
}


func (s *ServOVSDB) Set_db_change_aware(param *interface{}, reply *interface{}) error {
	fmt.Printf("Set_db_change_aware %+v\n", *param)
	*reply = "{Set_db_change_aware}"
	return nil
}

func (s *ServOVSDB) Convert(param *interface{}, reply *interface{}) error {
	fmt.Printf("Convert %+v\n", *param)
	*reply = "{Convert}"
	return nil
}

func (s *ServOVSDB) Echo(line []byte, reply *interface{}) error {
	fmt.Printf("Echo %s\n", string(line))
	*reply = string(line)
	return nil
}

func NewService(dbServer *DBServer) *ServOVSDB {
	return &ServOVSDB{dbServer: *dbServer}
}