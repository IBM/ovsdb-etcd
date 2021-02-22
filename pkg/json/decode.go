package json

import (
	"encoding/json"
	"fmt"
	"strings"
)

//func (u Uuid) UnMarshalJSON(bytes []byte) error {
//	//return json.unMarshal([]string{"uuid", string(u)})
//	var res Uuid
//	err := json.Unmarshal(bytes,&res)
//	if (err != nil){
//		return err
//	}
//	return nil
//}
//

func (v *Uuid) UnmarshalJSON(bytes []byte) error {
	var s string
	err := json.Unmarshal(bytes, &s)
	if err != nil {
		return err
	}
	//*v=Uuid(s[1])
	return nil
}

//naive unmarshller
func (v *NB_Global2_3) UnmarshalJSON(bytes []byte) error {

	res := naiveCustomBraketsRemove(bytes)
	var s map[string]interface{}
	err := json.Unmarshal(res, &s)
	if err != nil {
		return err
	}

	v.Name          =s["name"].(string)
	v.NbCfg         =int(s["nb_cfg"].(float64))
	v.NbCfgTimestamp=int(s["nb_cfg_timestamp"].(float64))
	v.SbCfg         =int(s["sb_cfg"].(float64))
	v.SbCfgTimestamp=int(s["sb_cfg_timestamp"].(float64))
	v.HvCfg         =int(s["hv_cfg"].(float64))
	v.HvCfgTimestamp=int(s["hv_cfg_timestamp"].(float64))
	v.ExternalIds = naiveConvertToStringToStringMap(s["external_ids"].([]interface{}))
	v.Connections   =s["connections"].([]interface{})
	//v.Ssl           =s["ssl"].([]interface{})
	v.Connections   =s["connections"].([]interface{})
	if val , ok :=s["ssl"];ok{
		v.Ssl           =val.([]interface{})
	}
	v.Options = naiveConvertToStringToStringMap(s["options"].([]interface{}))
	v.Ipsec         =s["ipsec"].(bool)

	return nil
}
//naive unmarshller
func (v *NB_Global2_5) UnmarshalJSON(bytes []byte) error {

	res := naiveCustomBraketsRemove(bytes)
	var s map[string]interface{}
	err := json.Unmarshal(res, &s)
	if err != nil {
		return err
	}

	v.Name          =s["name"].(string)
	v.NbCfg         =int(s["nb_cfg"].(float64))
	v.NbCfgTimestamp=int(s["nb_cfg_timestamp"].(float64))
	v.SbCfg         =int(s["sb_cfg"].(float64))
	v.SbCfgTimestamp=int(s["sb_cfg_timestamp"].(float64))
	v.HvCfg         =int(s["hv_cfg"].(float64))
	v.HvCfgTimestamp=int(s["hv_cfg_timestamp"].(float64))
	v.ExternalIds = naiveConvertToStringToStringMap(s["external_ids"].([]interface{}))
	v.Options = naiveConvertToStringToStringMap(s["options"].([]interface{}))
	v.Ipsec         =s["ipsec"].(bool)

	return nil
}

//tmp:to write better brakets remove
//VERY NAIVE IMPLEMENTATION
func naiveCustomBraketsRemove(bytes []byte) []byte{
	s := string(bytes)
	//splitted := strings.Split(s, `["map",[`)
	//splitted_0 := strings.ReplaceAll(s, `"external_ids":["map",[]],`,``)
	splitted_0 := strings.ReplaceAll(s, `,"ssl":["set",[]]`,``) //assuming set is not first
	splitted_1 := strings.ReplaceAll(splitted_0, `["map",[`,`[`)
	splitted_1_5 := strings.ReplaceAll(splitted_1, `["set",[`,`[`)
	splitted_2 := strings.ReplaceAll(splitted_1_5, `]],`,`],`)
	splitted_3 := strings.ReplaceAll(splitted_2, `"connections":"`,`"connections":["`)
	splitted_4 := strings.ReplaceAll(splitted_3, `","ssl`,`"],"ssl`)//assuming ssl comes always imidiatly after connections

	splitted_5 := strings.ReplaceAll(splitted_4, `"ssl":"`,`"ssl":["`)
	splitted_6 := strings.ReplaceAll(splitted_5, `","options`,`"],"options`)//assuming options comes always imidiatly after ssl
	fmt.Printf(splitted_6)
	return []byte(splitted_6)
}

func naiveConvertToStringToStringMap(i []interface{}) map[string]string{
	if len(i)==0 {
		return nil
	}
	res_m := make(map[string]string)
	for _,item:=range i {
		r := strings.Split(strings.Trim(fmt.Sprintf("%v",item),"[]")," ")
		res_m[r[0]]=r[1]
	}
	return res_m
}

/*
func (v *map[string]) UnmarshalJSON(bytes []byte) error {
	var s string
	err := json.Unmarshal(bytes, &s)
	if err != nil {
		return err
	}
	//*v=Uuid(s[1])
	return nil
}
*/
//func (n *NB_Global) UnmarshalJSON(bytes []byte) error {
//	//var tmp NB_Global
//	var tmp map[string]interface{}
//	err := json.Unmarshal(bytes, &tmp)
//	if err != nil {
//		return err
//	}
//	//n.Uuid=tmp[]
//	n.Uuid=Uuid(tmp[`_uuid`])
//	n.NamedUuid=tmp["_named_uuid"]
//	n.Name          =tmp["name"]
//	n.NbCfg         =tmp["nb_cfg"]
//	n.NbCfgTimestamp=tmp["nb_cfg_timestamp"]
//	n.SbCfg         =tmp["sb_cfg"]
//	n.SbCfgTimestamp=tmp["sb_cfg_timestamp"]
//	n.HvCfg         =tmp["hv_cfg"]
//	n.HvCfgTimestamp=tmp["hv_cfg_timestamp"]
//	n.ExternalIds   =tmp["external_ids"]
//	n.Connections   =tmp["connections"]
//	n.Ssl           =tmp["ssl"]
//	n.Options       =tmp["options"]
//	n.Ipsec         =tmp["ipsec"]
//
//
//
//
//	return nil
//}
/*
func (n *NB_Global2) UnmarshalJSON(bytes []byte) error {
	//var tmp NB_Global
	var tmp map[string]interface{}
	err := json.Unmarshal(bytes, &tmp)
	if err != nil {
		return err
	}
	//n.Uuid=Uuid(tmp[`_uuid`])
	//n.NamedUuid=tmp["_named_uuid"]
	n.Name          =tmp["name"]
	n.NbCfg         =tmp["nb_cfg"]
	n.NbCfgTimestamp=tmp["nb_cfg_timestamp"]
	n.SbCfg         =tmp["sb_cfg"]
	n.SbCfgTimestamp=tmp["sb_cfg_timestamp"]
	n.HvCfg         =tmp["hv_cfg"]
	n.HvCfgTimestamp=tmp["hv_cfg_timestamp"]
	n.ExternalIds   =tmp["external_ids"]
	n.Connections   =tmp["connections"]
	n.Ssl           =tmp["ssl"]
	n.Options       =tmp["options"]
	n.Ipsec         =tmp["ipsec"]




	return nil
}
*/
