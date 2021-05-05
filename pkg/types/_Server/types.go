package _Server

import "github.com/ibm/ovsdb-etcd/pkg/libovsdb"

type Database struct {
	Cid       libovsdb.OvsSet `json:"cid,omitempty"`
	Connected bool            `json:"connected,omitempty"`
	Index     libovsdb.OvsSet `json:"index,omitempty"`
	Leader    bool            `json:"leader,omitempty"`
	Model     string          `json:"model,omitempty"`
	Name      string          `json:"name,omitempty"`
	Schema    libovsdb.OvsSet `json:"schema,omitempty"`
	Sid       libovsdb.OvsSet `json:"sid,omitempty"`
	Version   libovsdb.UUID   `json:"_version,omitempty"`
	Uuid      libovsdb.UUID   `json:"_uuid,omitempty"`
}
