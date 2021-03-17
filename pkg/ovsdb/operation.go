package ovsdb

import (
	"encoding/json"

	"github.com/ebay/libovsdb"
)

type doOperation struct {
	db Databaser
}

func (doOp *doOperation) Insert(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	return nil, nil
}

func (doOp *doOperation) Select(op *libovsdb.Operation) (*libovsdb.OperationResult, error) {
	columns := []interface{}{}
	for _, val := range op.Columns {
		columns = append(columns, val)
	}

	mapResults, err := doOp.db.GetMarshaled(op.Table, columns)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(mapResults)   // FIXME: handle err
	result := libovsdb.OperationResult{} // XXX(alexey): check json representation
	err = json.Unmarshal(b, &result)     // FIXME: handle err
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
