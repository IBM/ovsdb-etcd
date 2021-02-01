package ovsdb

import (
	"encoding/json"
	"fmt"
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

func (s *ServOVSDB) Monitor_cond(param []interface{}, reply *interface{}) error {
	fmt.Printf("Monitor_cond %T %+v\n", param, param)
	//f := *param
	for k, v := range param {
		fmt.Printf("k = %v v = %v \n", k, v)
	}
	*reply = "{Monitor_cond}"
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