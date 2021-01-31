package ovsdb

import (
	"encoding/json"
	"io/ioutil"
)
type ServOVSDB struct {
	schemas map[string]string
}


func (s *ServOVSDB) Get_schema(line string, reply *interface{}) error {
	var f interface{}
	err := json.Unmarshal([]byte(s.schemas[line]), &f)
	if err != nil {
		return err
	}
	*reply = f
	return nil
}

func AddSchema(serv *ServOVSDB, schemaName, schemaFile string) error {
	data, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return err
	}
	serv.schemas[schemaName] =  string(data)
	return nil
}

func NewService() *ServOVSDB {
	return &ServOVSDB{schemas:make(map[string]string)}
}