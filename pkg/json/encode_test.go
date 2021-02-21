package json

import (
	"encoding/json"
	"fmt"
	"testing"

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

var nb NB_Global = NB_Global{
	Uuid:        uuid,
	NamedUuid:   namedUuid,
	Connections: Set{"71a565df-188f-42d0-9f18-74e180e27889"},
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
