package ovsdb

import (
	"encoding/json"

	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
)

type doOperation struct {
	dbname string
	db     Databaser
}

func (doOp *doOperation) Insert(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Select(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	result := libovsdb.OperationResult{} // XXX(alexey): check json representation

	// FIXME: reduce number of rows using 'op.Where'
	// FIXME: reduce size of each row using 'op.Columns'
	prefix := doOp.dbname + "/" + op.Table
	resp, err := doOp.db.GetData(prefix, false)
	if err != nil {
		return &result, err
	}

	resultMap := map[string]interface{}{
		"rows": resp.Kvs,
	}

	b, err := json.Marshal(resultMap) // FIXME: handle err
	err = json.Unmarshal(b, &result)  // FIXME: handle err
	return &result, nil
}

func (doOp *doOperation) Update(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Mutate(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Delete(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Wait(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Commit(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Abort(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Comment(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Assert(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}
