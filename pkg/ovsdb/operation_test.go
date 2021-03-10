package ovsdb

import (
	"testing"

	"github.com/ebay/libovsdb"
	"github.com/stretchr/testify/assert"
)

func NewOperationMock() *doOperation {
	doOp := &doOperation{}
	doOp.dbServer, _ = NewDBServerMock()
	return doOp
}

func TestOperationSelect(t *testing.T) {
	doOp := NewOperationMock()
	op := &libovsdb.Operation{}
	op.Op = "select"
	_, err := doOp.Select(op)
	assert.Nil(t, err)
}
