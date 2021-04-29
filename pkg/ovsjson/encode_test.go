package ovsjson

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/stretchr/testify/assert"
)

var expectedUuid string = `[` +
	`"uuid",` +
	`"a5088a51-7756-4dd4-909c-b7c59c9fcce7"` +
	`]`

var expectedNamedUuid string = `[` +
	`"named-uuid",` +
	`"myuuid"` +
	`]`

var expectedOptions string = `["map",[` +
	`["mac_prefix","0e:f6:a4"],` +
	`["max_tunid","16711680"],` +
	`["northd_internal_version","20.12.0-20.14.0-52.0"],` +
	`["northd_probe_interval","5000"],` +
	`["svc_monitor_mac","e2:d2:ac:ad:7b:8a"],` +
	`["e2e_timestamp","1612966696"]` +
	`]]`

var expected string = `{` +
	`"_uuid":` + expectedUuid + `,` +
	`"_named_uuid":` + expectedNamedUuid + `,` +
	`"name":"",` +
	`"nb_cfg":0,` +
	`"nb_cfg_timestamp":0,` +
	`"sb_cfg":0,` +
	`"sb_cfg_timestamp":0,` +
	`"hv_cfg":0,` +
	`"hv_cfg_timestamp":0,` +
	`"external_ids":["map",[]],` +
	`"connections":["uuid","71a565df-188f-42d0-9f18-74e180e27889"],` +
	`"ssl":["set",[]],` +
	`"options":` + expectedOptions + `,` +
	`"ipsec":false` +
	`}`

var uuid Uuid = "a5088a51-7756-4dd4-909c-b7c59c9fcce7"

var namedUuid NamedUuid = "myuuid"

var options Map = Map{
	"e2e_timestamp":           "1612966696",
	"mac_prefix":              "0e:f6:a4",
	"max_tunid":               "16711680",
	"northd_internal_version": "20.12.0-20.14.0-52.0",
	"northd_probe_interval":   "5000",
	"svc_monitor_mac":         "e2:d2:ac:ad:7b:8a",
}

type NB_Global struct {
	Uuid           Uuid      `json:"_uuid"`
	NamedUuid      NamedUuid `json:"_named_uuid"`
	Name           string    `json:"name"`
	NbCfg          int       `json:"nb_cfg"`
	NbCfgTimestamp int       `json:"nb_cfg_timestamp"`
	SbCfg          int       `json:"sb_cfg"`
	SbCfgTimestamp int       `json:"sb_cfg_timestamp"`
	HvCfg          int       `json:"hv_cfg"`
	HvCfgTimestamp int       `json:"hv_cfg_timestamp"`
	ExternalIds    Map       `json:"external_ids"`
	Connections    Uuid      `json:"connections"`
	Ssl            Set       `json:"ssl"`
	Options        Map       `json:"options"`
	Ipsec          bool      `json:"ipsec"`
}

var nb NB_Global = NB_Global{
	Uuid:        uuid,
	NamedUuid:   namedUuid,
	Connections: "71a565df-188f-42d0-9f18-74e180e27889",
	Options:     options,
}

func TestUuid(t *testing.T) {
	b, _ := json.Marshal(uuid)
	actual := string(b)
	assert.Equal(t, expectedUuid, actual, "they should be equal")
}

func TestNamedUuid(t *testing.T) {
	b, _ := json.Marshal(namedUuid)
	actual := string(b)
	assert.Equal(t, expectedNamedUuid, actual, "they should be equal")
}

func TestOptionsLength(t *testing.T) {
	b, _ := json.Marshal(options)
	actual := string(b)
	assert.Equal(t, len(expectedOptions), len(actual), "they should be same length\n"+
		fmt.Sprintf("expected: %s\n", expectedOptions)+
		fmt.Sprintf("actual  : %s\n", actual))
}

func TestLength(t *testing.T) {
	b, _ := json.Marshal(nb)
	actual := string(b)
	assert.Equal(t, len(expected), len(actual), "they should be same length\n"+
		fmt.Sprintf("expected: %s\n", expected)+
		fmt.Sprintf("actual  : %s\n", actual))
}

// TODO
func TestCondMonitorParameters(t *testing.T) {
	// TODO add more tess
	// several databases; different select watch, columns and the last transaction options options
	// ["_Server",null,{"Database":[{"where":[["model","==","standalone"],true],"select":{"modify":false,"initial":true,"insert":true,"delete":true},"columns":["model","connected"]}]}]
	// ["_Server",null,{"Database":[{"where":[["model","==","standalone"],true],"columns":["cid","connected","index","leader","model","name","schema","sid","_version"]}]},"00000000-0000-0000-0000-000000000000"]
	// ["_Server",null,{"Database":[{"where":[true],"columns":["cid","connected","index","leader","model","name","schema","sid","_version"]}]},"00000000-0000-0000-0000-000000000000"]
	var s = []byte(`["_Server",null,{"Database":[{"select":{"modify":true,"initial":true,"insert":false,"delete":true},"columns":["model"]}]}]`)
	actualCMP := CondMonitorParameters{}
	err := json.Unmarshal(s, &actualCMP)
	assert.Nil(t, err)
	expectedSelect := &libovsdb.MonitorSelect{Modify: libovsdb.Bool(true), Initial: libovsdb.Bool(true), Insert: libovsdb.Bool(false), Delete: libovsdb.Bool(true)}
	mcr := MonitorCondRequest{Columns: []string{"model"},
		Select: expectedSelect}

	expectedCMP := CondMonitorParameters{DatabaseName: "_Server", MonitorCondRequests: map[string][]MonitorCondRequest{"Database": {mcr}}}
	assert.Equal(t, expectedCMP, actualCMP, "they should be equal\n"+
		fmt.Sprintf("expected: %v\n", expectedCMP)+
		fmt.Sprintf("actual  : %v\n", actualCMP))

}
