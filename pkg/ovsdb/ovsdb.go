package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/klog/v2"
)

type ServOVSDB struct {
	dbServer DBServerInterface
}

func (s *ServOVSDB) ListDbs(ctx context.Context, param interface{}) ([]string, error) {
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

func (s *ServOVSDB) GetServerId(ctx context.Context) string {
	klog.V(5).Infof("GetServerId request")
	return s.dbServer.GetUUID()
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
	return &ServOVSDB{
		dbServer: dbServer,
	}
}
