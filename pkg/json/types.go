package json

type Uuid string

type NamedUuid string

type Map map[string]string

type Set []interface{}

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
	Connections    Set       `json:"connections"`
	Ssl            Set       `json:"ssl"`
	Options        Map       `json:"options"`
	Ipsec          bool      `json:"ipsec"`
}
//tmp types
type NB_Global2 struct {
	//Uuid           Uuid      `json:"_uuid"`
	//NamedUuid      NamedUuid `json:"_named_uuid"`
	Name           string    `json:"name"`
	NbCfg          int       `json:"nb_cfg"`
	NbCfgTimestamp int       `json:"nb_cfg_timestamp"`
	SbCfg          int       `json:"sb_cfg"`
	SbCfgTimestamp int       `json:"sb_cfg_timestamp"`
	HvCfg          int       `json:"hv_cfg"`
	HvCfgTimestamp int       `json:"hv_cfg_timestamp"`
	ExternalIds    Map       `json:"external_ids"`
	Connections    Set       `json:"connections"`
	Ssl            Set       `json:"ssl"`
	Options        Map       `json:"options"`
	Ipsec          bool      `json:"ipsec"`
}

type NB_Global3 struct {
	//Uuid           Uuid      `json:"_uuid"`
	//NamedUuid      NamedUuid `json:"_named_uuid"`
	Name           string    `json:"name"`
	NbCfg          int       `json:"nb_cfg"`
	NbCfgTimestamp int       `json:"nb_cfg_timestamp"`
	SbCfg          int       `json:"sb_cfg"`
	SbCfgTimestamp int       `json:"sb_cfg_timestamp"`
	HvCfg          int       `json:"hv_cfg"`
	HvCfgTimestamp int       `json:"hv_cfg_timestamp"`
	//ExternalIds    Map       `json:"external_ids"`
	//Connections    Set       `json:"connections"`
	//Ssl            Set       `json:"ssl"`
	//Options        Map       `json:"options"`
	Ipsec          bool      `json:"ipsec"`
}
