package common

import (
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

func ToUUID(s string) libovsdb.UUID {
	return libovsdb.UUID{GoUUID: s}
}

func ToUUIDSlice(v []string) []libovsdb.UUID {
	res := make([]libovsdb.UUID, len(v))
	for i := range res {
		res[i] = ToUUID(v[i])
	}
	return res
}
