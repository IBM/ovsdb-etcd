package common

import (
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

func BytesToInterface(in []byte) *interface{} {
	var result interface{}
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToMapInterface(in []byte) *map[string]interface{} {
	var result map[string]interface{}
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToMapString(in []byte) *map[string]string {
	var result map[string]string
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToArrayMapString(in []byte) *[]map[string]string {
	var result []map[string]string
	json.Unmarshal([]byte(in), &result)
	return &result
}

func BytesToArrayInterface(in []byte) *[]interface{} {
	var result []interface{}
	json.Unmarshal([]byte(in), &result)
	return &result
}

func MapToEtcdResponse(in *map[string]interface{}) (*clientv3.GetResponse, error) {
	kvs := []*mvccpb.KeyValue{}
	for k, v := range *in {
		value, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &mvccpb.KeyValue{Key: []byte(k), Value: value})
	}
	return &clientv3.GetResponse{Kvs: kvs}, nil
}

func BytesToOperation(in []byte) *libovsdb.Operation {
	var result libovsdb.Operation
	json.Unmarshal([]byte(in), &result)
	return &result
}

func StringArrayToMap(in []string) map[string]bool {
	ret := map[string]bool{}
	for _, str := range in {
		ret[str] = true
	}
	return ret
}

func ArrayMapStringToArrayInterface(in []map[string]string) (out []interface{}) {
	for _, v := range in {
		out = append(out, v)
	}
	return
}

func ParamsToString(param interface{}) (string, error) {
	switch param.(type) {
	case []interface{}:
		intArray := param.([]interface{})
		if len(intArray) == 0 {
			klog.Warningf("Empty params")
			return "", fmt.Errorf("Empty params")
		} else {
			return fmt.Sprintf("%s", intArray[0]), nil
		}
	case string:
		return param.(string), nil
	default:
		return fmt.Sprintf("%s", param), nil
	}
}
