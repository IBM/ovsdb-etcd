package json

import "encoding/json"

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
