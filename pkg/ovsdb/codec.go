package ovsdb

import (
	"io"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"
)

type Codec struct {
	code rpc.ServerCodec
}

func (c *Codec) ReadRequestHeader(r *rpc.Request) error {
	e := c.code.ReadRequestHeader(r)
	if e!= nil {
		return e
	}
	dot := strings.LastIndex(r.ServiceMethod, ".")
	needClass := false
	if dot < 0 {
		needClass = true
	}
	methodName := r.ServiceMethod[dot+1:]
	startLetter := methodName[:1]
	startLetter = strings.ToUpper(startLetter)
	methodName = startLetter + methodName[1:]
	if needClass {
		r.ServiceMethod = "ovsdb." + methodName
	}  else {
		r.ServiceMethod = r.ServiceMethod[:dot] + methodName
	}
	return e
}

func (c *Codec) ReadRequestBody(x interface{}) error {
	return c.code.ReadRequestBody(x)
}

func (c *Codec) WriteResponse(r *rpc.Response, x interface{}) error {
	return c.code.WriteResponse(r, x)
}

func (c *Codec) Close() error {
	return c.code.Close()
}

func NewCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &Codec{ code: jsonrpc.NewServerCodec(conn)}
}
