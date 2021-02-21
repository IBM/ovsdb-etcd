package json_test

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	. "github.com/roytman/ovsdb-etcd/pkg/json"
)

func TestJson(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Json Suite")
}

var _ = Describe("jason encoder", func() {

	var(
		uuid Uuid = "a5088a51-7756-4dd4-909c-b7c59c9fcce7"
		namedUuid NamedUuid = "myuuid"
		options = Map{
			"e2e_timestamp":           "1612966696",
			"mac_prefix":              "0e:f6:a4",
			"max_tunid":               "16711680",
			"northd_internal_version": "20.12.0-20.14.0-52.0",
			"northd_probe_interval":   "5000",
			"svc_monitor_mac":         "e2:d2:ac:ad:7b:8a",
		}
		nb = NB_Global {
			Uuid:        uuid,
			NamedUuid:   namedUuid,
			Connections: Set{"71a565df-188f-42d0-9f18-74e180e27889"},
			Options:     options,
		}
	)

	var(
		expectedUuid = `[` +
			`"uuid",` +
			`"a5088a51-7756-4dd4-909c-b7c59c9fcce7"` +
			`]`

		expectedNamedUuid = `[` +
		`"named-uuid",` +
		`"myuuid"` +
		`]`

		expectedOptions = `["map",[` +
		`["mac_prefix","0e:f6:a4"],` +
		`["max_tunid","16711680"],` +
		`["northd_internal_version","20.12.0-20.14.0-52.0"],` +
		`["northd_probe_interval","5000"],` +
		`["svc_monitor_mac","e2:d2:ac:ad:7b:8a"],` +
		`["e2e_timestamp","1612966696"]` +
		`]]`

		expectedNb = `{` +
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
		`"connections":"71a565df-188f-42d0-9f18-74e180e27889",` +
		`"ssl":["set",[]],` +
		`"options":` + expectedOptions + `,` +
		`"ipsec":false` +
		`}`
	)

	DescribeTable("the marshall operation (content should be equal)",
		func(input interface{}, expected string) {
			b, _ := json.Marshal(input)
			actual := string(b)
			Expect(actual).To(Equal(expected))
		},

		Entry("uuid ",uuid,expectedUuid),
		Entry("named uuid",namedUuid,expectedNamedUuid),
	)

	DescribeTable("the marshall operation (length should be equal)",
		func(input interface{}, expected string) {
			b, _ := json.Marshal(input)
			actual := string(b)
			Expect(len(actual)).To(Equal(len(expected)))
		},

		Entry("options",options,expectedOptions),
		Entry("nb",nb,expectedNb),
	)
})
