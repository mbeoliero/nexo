package gateway

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"uuid"

	"github.com/mbeoliero/nexo/errcode"
)

// Client → server request ids and server → client push ids (design §7.2).
const (
	ReqGetMaxSeqs        = 1001
	ReqPullMsgBySeqRange = 1002
	ReqSendMsg           = 1003
	ReqMarkRead          = 1004

	PushMsg    = 2001
	KickOnline = 2002
	ConvRead   = 2003
	Resync     = 2004
)

// Wire format is a JSON text frame; data is a nested object, never base64.
type Request struct {
	ReqId   int            `json:"req_id"`
	OpId    string         `json:"op_id"`
	MsgIncr string         `json:"msg_incr,omitempty"`
	Data    jsontext.Value `json:"data,omitempty"`
}

type Response struct {
	ReqId   int    `json:"req_id"`
	OpId    string `json:"op_id"`
	MsgIncr string `json:"msg_incr,omitempty"`
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
	Data    any    `json:"data"`
}

type Push struct {
	ReqId int    `json:"req_id"`
	OpId  string `json:"op_id"`
	Data  any    `json:"data"`
}

func decodeRequest(raw []byte) (Request, error) {
	var req Request
	if err := json.Unmarshal(raw, &req); err != nil {
		return req, errcode.ErrInvalidProtocol.Wrap(err)
	}
	if req.ReqId == 0 {
		return req, errcode.ErrInvalidProtocol.WithMessage("req_id is required")
	}
	return req, nil
}

func (r Request) reply(data any) []byte {
	return mustMarshal(Response{ReqId: r.ReqId, OpId: r.OpId, MsgIncr: r.MsgIncr, Data: data})
}

func (r Request) fail(err error) []byte {
	e := errcode.From(err)
	return mustMarshal(Response{ReqId: r.ReqId, OpId: r.OpId, MsgIncr: r.MsgIncr, Code: e.Code, Message: e.Message, Data: struct{}{}})
}

func pushFrame(reqId int, data any) []byte {
	return mustMarshal(Push{ReqId: reqId, OpId: uuid.NewV7().String(), Data: data})
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("gateway: marshal frame: " + err.Error())
	}
	return b
}
