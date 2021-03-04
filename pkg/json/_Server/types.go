package _Server

import "github.com/ibm/ovsdb-etcd/pkg/json"

type Database struct {
	Cid       json.Uuid `json:"cid,omitempty"`
	Connected bool      `json:"connected,omitempty"`
	Index     int64     `json:"index,omitempty"`
	Leader    bool      `json:"leader,omitempty"`
	Model     string    `json:"model,omitempty"`
	Name      string    `json:"name,omitempty"`
	Schema    string    `json:"schema,omitempty"`
	Sid       json.Uuid `json:"sid,omitempty"`
	Version   json.Uuid `json:"_version,omitempty"`
	Uuid      json.Uuid `json:"uuid,omitempty"`
}
