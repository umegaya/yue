package yue

import (
	"fmt"

	proto "yue/proto"
)


//
// payload
//
const (
	REQUEST = iota	//kind, msgid, pid, method, args
	RESPONSE		//kind, msgid, err, args
	NOTIFY 			//kind, pid, method, args

	TXN = 0x4
)

const (
	KIND = 0
	
	REQUEST_MSGID = 1
	REQUEST_PID = 2
	REQUEST_METHOD = 3
	REQUEST_ARGS = 4
	REQUEST_CTX = 5
	REQUEST_RECEIVER = 6

	RESPONSE_MSGID = 1
	RESPONSE_ERROR = 2
	RESPONSE_ARGS = 3
	
	NOTIFY_PID = 1
	NOTIFY_METHOD = 2
	NOTIFY_ARGS = 3
)

type payload struct {
	kind uint8
	data interface{}
}

func (pl *payload) request() *request {
	return pl.data.(*request)
}
func (pl *payload) notify() *notify {
	return pl.data.(*notify)
}
func (pl *payload) response() *response {
	return pl.data.(*response)
}

func (pl *payload) String() string {
	switch (pl.kind) {
	case REQUEST:
		return fmt.Sprintf("q[%s]", pl.request().String())
	case RESPONSE:
		return fmt.Sprintf("r[%s]", pl.response().String())
	case NOTIFY:
		return fmt.Sprintf("n[%s]", pl.notify().String())
	default:
		return fmt.Sprintf("?[%v]", pl.data)
	}
}

type request struct {
	msgid proto.MsgId
	pid proto.ProcessId
	method string
	args []interface{}
	//following datas are not serialized.
	ctx *rpcContext
	receiver chan *response
}

func (req *request) String() string {
	return fmt.Sprintf("%v,%v,%v,%v", req.msgid, req.pid, req.method, req.args)
}

type notify struct {
	pid proto.ProcessId
	method string
	args []interface{}	
}

func (n *notify) String() string {
	return fmt.Sprintf("%v,%v,%v", n.pid, n.method, n.args)
}

type response struct {
	msgid proto.MsgId
	error *ActorError
	args interface{}
}

func (res *response) String() string {
	if res.error != nil {
		return fmt.Sprintf("%v,%v,%v", res.msgid, res.error.Error(), res.args)
	} else {
		return fmt.Sprintf("%v,nil(error),%v", res.msgid, res.args)
	}
}

func buildRequest(ctx *rpcContext, msgid proto.MsgId, pid proto.ProcessId, method string, args []interface {}, ch chan *response) *payload {
	return &payload {
		kind: REQUEST,
		data: &request {
			msgid: msgid, 
			pid: pid, 
			method: method, 
			args: args,
			ctx: ctx,
			receiver: ch,
		},
	}
}

func buildResponse(msgid proto.MsgId, err *ActorError, args interface {}) *payload {
	return &payload {
		kind: RESPONSE,
		data: &response {
			msgid: msgid, 
			args: args,
			error: err,
		},
	}
}

func buildNotify(pid proto.ProcessId, method string, args []interface {}) *payload {
	return &payload {
		kind: NOTIFY,
		data: &notify {
			pid: pid,
			method: method, 
			args: args,
		},
	}
}
