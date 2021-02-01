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

func (s *ServOVSDB) Get_schema(line string, reply *interface{}) error {
	var f interface{}
	err := json.Unmarshal([]byte(s.dbServer.schemas[line]), &f)
	if err != nil {
		return err
	}
	*reply = f
	return nil
}

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

func (s *ServOVSDB) Monitor_cond_since(param *interface{}, reply *interface{}) error {
	fmt.Printf("Monitor_cond %+v\n", *param)

	*reply = "{Monitor_cond_since}"
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