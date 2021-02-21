package json_test

import (
	"encoding/json"
	"fmt"
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
		uuidObj Uuid = "a5088a51-7756-4dd4-909c-b7c59c9fcce7"
		namedUuidObj NamedUuid = "myuuid"
		optionsObj = Map{
			"e2e_timestamp":           "1612966696",
			"mac_prefix":              "0e:f6:a4",
			"max_tunid":               "16711680",
			"northd_internal_version": "20.12.0-20.14.0-52.0",
			"northd_probe_interval":   "5000",
			"svc_monitor_mac":         "e2:d2:ac:ad:7b:8a",
		}
		nbObj = NB_Global {
			Uuid:        uuidObj,
			NamedUuid:   namedUuidObj,
			Connections: Set{"71a565df-188f-42d0-9f18-74e180e27889"},
			Options:     optionsObj,
		}
		/*
		nbObj2 = NB_Global2 {
			Connections: Set{"71a565df-188f-42d0-9f18-74e180e27889"},
			Options:     optionsObj,
		}
		*/
		nbObj3 = NB_Global3 {
			//Connections: Set{"71a565df-188f-42d0-9f18-74e180e27889"},
			//Options:     optionsObj,
		}
	)

	var(
		uuidJson = `[` +
			`"uuid",` +
			`"a5088a51-7756-4dd4-909c-b7c59c9fcce7"` +
			`]`

		namedUuidJson = `[` +
		`"named-uuid",` +
		`"myuuid"` +
		`]`

		optionsJson = `["map",[` +
		`["mac_prefix","0e:f6:a4"],` +
		`["max_tunid","16711680"],` +
		`["northd_internal_version","20.12.0-20.14.0-52.0"],` +
		`["northd_probe_interval","5000"],` +
		`["svc_monitor_mac","e2:d2:ac:ad:7b:8a"],` +
		`["e2e_timestamp","1612966696"]` +
		`]]`

		nbJson = `{` +
		`"_uuid":` + uuidJson + `,` +
		`"_named_uuid":` + namedUuidJson + `,` +
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
		`"options":` + optionsJson + `,` +
		`"ipsec":false` +
		`}`

		/*
		nbJson2 = `{` +
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
			`"options":` + optionsJson + `,` +
			`"ipsec":false` +
			`}`
		*/

		nbJson3 = `{` +
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
			`"options":` + optionsJson + `,` +
			`"ipsec":false` +
			`}`
	)

	DescribeTable("the marshall operation (content should be equal)",
		func(input interface{}, expected string) {
			b, _ := json.Marshal(input)
			actual := string(b)
			Expect(actual).To(Equal(expected))
		},

		Entry("uuid ",uuidObj,uuidJson),
		Entry("named uuid",namedUuidObj,namedUuidJson),
	)

	DescribeTable("the marshall operation (length should be equal)",
		func(input interface{}, expected string) {
			b, _ := json.Marshal(input)
			actual := string(b)
			Expect(len(actual)).To(Equal(len(expected)))
		},

		Entry("options",optionsObj,optionsJson),
		Entry("nb",nbObj,nbJson),
	)

	DescribeTable("the unmarshall operation (content should be equal)",
		func(input string, expected interface{}) {
			//var v interface{}
			var v Uuid
			json.Unmarshal([]byte(input),v)
			//b, _ := json.Marshal(input)
			//actual := string(b)
			Expect(v).To(Equal(expected))
		},

		//Entry("uuid ",uuidJson,uuidObj),
		//Entry("named uuid",namedUuidJson,namedUuidObj),
	)

	DescribeTable("the unmarshall operation (just testing)",
		func(input string, expected interface{}) {
			alice := Elephant{}
			aliceJson := `{"name":"Alice","age":2}`
			err := json.Unmarshal([]byte(aliceJson), &alice)
			if err != nil {
				panic(err)
			}
			fmt.Printf("%#v\n", alice)
		},

		//Entry("uuid ",uuidJson,uuidObj),
		//Entry("named uuid",namedUuidJson,namedUuidObj),
	)
	DescribeTable("the unmarshall operation (just testing3)",
		func(input string, expected interface{}) {

			//v := NB_Global2{}
			//var v interface{}
			var v NB_Global3

			err := json.Unmarshal([]byte(input), &v)
			if err != nil {
				panic(err)
			}
			fmt.Printf("%#v\n", v)
			Expect(v).To(Equal(expected))
		},

		Entry("uuid ",nbJson3,nbObj3),
		//Entry("uuid ",nbJson2,nbObj2),
		//Entry("named uuid",namedUuidJson,namedUuidObj),
	)
})

type Name struct {
	CommonName     string
	ScientificName string
}

func (n *Name) UnmarshalJSON(bytes []byte) error {
	var name string
	err := json.Unmarshal(bytes, &name)
	if err != nil {
		return err
	}
	n.CommonName = name
	n.ScientificName = ""
	return nil
}

type Elephant struct {
	Name Name `json:"name"`
	Age  int  `json:"age"`
}
