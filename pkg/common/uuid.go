package common

import (
	ovsdbjson "github.com/ebay/libovsdb"
)

func ToUUID(s string) ovsdbjson.UUID {
	return ovsdbjson.UUID{GoUUID: s}
}

func ToUUIDSlice(v []string) []ovsdbjson.UUID {
	res := make([]ovsdbjson.UUID, len(v))
	for i := range res {
		res[i] = ToUUID(v[i])
	}
	return res
}
