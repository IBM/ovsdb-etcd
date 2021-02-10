package main

import (
	"encoding/json"
	"fmt"
)

type Uuid string

func (u Uuid) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{"uuid", string(u)})
}

type Map map[string]string

func (m Map) MarshalJSON() ([]byte, error) {
	if m == nil || len(m) == 0{
		return []byte("[\"map\",[]]"), nil
	}
	retStr := "[\"map\", ["
	for k, v := range m {
		b, _ := json.Marshal([]interface{}{k, v})
		retStr += string(b)
	}
	retStr += "]"
	return []byte(retStr), nil
}

type NB_Global struct {
	Uuid             Uuid            `json:"_uuid"`
	Name             string            `json:"name"`
	Nb_cfg           int               `json:"nb_cfg"`
	Nb_cfg_timestamp int               `json:"nb_cfg_timestamp"`
	Sb_cfg           int               `json:"sb_cfg"`
	Sb_cfg_timestamp int               `json:"sb_cfg_timestamp"`
	Hv_cfg           int               `json:"hv_cfg"`
	Hv_cfg_timestamp int               `json:"hv_cfg_timestamp"`
	External_ids     Map				`json:"external_ids"`
	Connections      Uuid            `json:"connections"`
	Ssl              []string          `json:"ssl"`
	Options          Map                `json:"options"`
	Ipsec            bool              `json:"ipsec"`
}

/*func (nbg *NB_Global) MarshalJSON() ([]byte, error) {
	if nbg.External_ids == nil {
		nbg.External_ids = Map{}
	}
	if nbg.Options == nil  {
		nbg.Options = Map{}
	}
	return json.Marshal(nbg)
}*/

func main() {
	nb := NB_Global{Uuid: "a5088a51-7756-4dd4-909c-b7c59c9fcce7", Connections: "413afe3e-79ff-4583-88a6-f02b70b8e927",
		Options: Map {"e2e_timestamp":"1612817071", "mac_prefix":"86:a9:cb", "max_tunid":"16711680", "northd_internal_version":"20.12.0-20.14.0-52.0", "northd_probe_interval":"5000", "svc_monitor_mac":"5a:d9:62:39:9f:87"}}
	fmt.Printf("%+v\n", nb)
	b, _ := json.Marshal(nb)
	fmt.Printf("NG %s\n", string(b))
}



