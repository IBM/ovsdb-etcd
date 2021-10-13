package ovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/klog/klogr"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

func TestConditionUUID(t *testing.T) {
	table := "table1"
	common.SetPrefix("ovsdb/nb")
	uuid := common.GenerateUUID()
	goUUID := libovsdb.UUID{GoUUID: uuid}

	cond := []interface{}{libovsdb.ColUuid, FuncEQ, goUUID}
	tableSchema, ok := testSchemaSimple.Tables[table]
	assert.True(t, ok)
	pCondition, err := NewCondition(&tableSchema, cond, klogr.New())
	assert.Nil(t, err)
	condition := *pCondition
	uuidStr, err := condition.getUUIDIfExists()
	assert.Nil(t, err)
	assert.Equal(t, uuid, uuidStr)
}

// TODO add tests
