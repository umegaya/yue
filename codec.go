package yue

import (
	"log"
	"net"
//	"fmt"
	"reflect"
	"bufio"
	"encoding/json"

	proto "github.com/umegaya/yue/proto"

	"github.com/umegaya/msgpack"
)

//register return type for each request, msgid basis.
var retvals map[proto.MsgId]interface{} = make(map[proto.MsgId]interface{})
var dummy interface{}
func registerRetval(msgid proto.MsgId, ret interface{}) {
	retvals[msgid] = ret
}
func getRetval(msgid proto.MsgId) interface{} {
	r, ok := retvals[msgid]
	if ok {
		delete(retvals, msgid)
		return r 
	} else {
		return &dummy
	}
}

//basic primitive decode/encode msgpack
//payload, request, notify, response
func (p *payload) EncodeMsgpack(enc *msgpack.Encoder) error {
	var l int
	switch (p.kind) {
	case REQUEST:
		l = 5
	case RESPONSE:
		l = 4
	case NOTIFY:
		l = 4
	}
	if err := enc.EncodeSliceLen(l); err != nil {
		return err
	}
	if err := enc.EncodeUint8(p.kind); err != nil {
		return err
	}
	switch (p.kind) {
	case REQUEST:
		return p.data.(*request).encode(enc)
	case RESPONSE:
		return p.data.(*response).encode(enc)
	case NOTIFY:
		return p.data.(*notify).encode(enc)
	}
	return newerr(ActorInvalidPayload, p.kind)
}

func (p *payload) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeSliceLen(); err != nil {
		return err
	}
	kind, err := dec.DecodeUint8()
	if err != nil {
		return err
	} 
	p.kind = kind
	switch (p.kind) {
	case REQUEST:
		req := &request{}
		p.data = req
		return req.decode(dec)
	case RESPONSE:
		res := &response{}
		p.data = res
		return res.decode(dec)
	case NOTIFY:
		n := &notify{}
		p.data = n
		return n.decode(dec)
	}
	return newerr(ActorInvalidPayload, p.kind)
}

//request
func (req *request) encode(enc *msgpack.Encoder) error {
	return enc.Encode(uint32(req.msgid), uint64(req.pid), req.method, req.args)
}

func (req *request) decode(dec *msgpack.Decoder) error {
	return dec.Decode(&req.msgid, &req.pid, &req.method, &req.args)
}

//response
func (res *response) encode(enc *msgpack.Encoder) error {
	return enc.Encode(uint32(res.msgid), res.error, res.args)
}

func (res *response) decode(dec *msgpack.Decoder) error {
	if err := dec.Decode(&res.msgid); err != nil {
		return err
	}
	res.args = getRetval(res.msgid)
	return dec.Decode(&res.error, res.args)
}

//notify
func (n *notify) encode(enc *msgpack.Encoder) error {
	return enc.Encode(uint64(n.pid), n.method, n.args)
}

func (n *notify) decode(dec *msgpack.Decoder) error {
	return dec.Decode(&n.pid, &n.method, &n.args)
}


//debug
type DumpEncoder struct {
	writer *bufio.Writer
}

func (e *DumpEncoder) Write(p []byte) (int, error) {
	log.Printf("dump write: %v", string(p))
	return e.writer.Write(p)
}

type DumpDecoder struct {
	reader *bufio.Reader
}

func (d *DumpDecoder) Read(p []byte) (int, error) {
	n, err := d.reader.Read(p)
	log.Printf("dump read: %v", string(p[:n]))
	return n, err
}

//codec interface
type Decoder interface {
	Decode(pl interface{}) error
}

type Encoder interface {
	Encode(pl interface{}) error
	Flush() error
}

type CodecFactory interface {
	NewDecoder(c net.Conn) Decoder
	NewEncoder(c net.Conn) Encoder
}

//implementation of DecoderFactory/EncoderFactory, by msgpack.
type MsgpackDecoder struct {
	msgpack.Decoder
	reader *bufio.Reader
}

func (e *MsgpackDecoder) Decode(v interface{}) error {
	return e.Decoder.Decode(v);
}

type MsgpackEncoder struct {
	msgpack.Encoder
	writer *bufio.Writer
}

func (e *MsgpackEncoder) Encode(v interface{}) error {
	return e.Encoder.Encode(v);
}

func (e *MsgpackEncoder) Flush() error {
	return e.writer.Flush()
}

type MsgpackCodecFactory struct {
	cfg func (interface{})
}

func NewMsgpackCodecFactory(cfg func (interface{})) *MsgpackCodecFactory {
	msgpack.Register(reflect.TypeOf(reflect.Value{}), 
		func (e *msgpack.Encoder, v reflect.Value) error {
			return e.EncodeValue(v.Interface().(reflect.Value))
		}, 
		func (d *msgpack.Decoder, v reflect.Value) error { //who wants to do this?
			vv := v.Interface().(reflect.Value) 
			return d.DecodeValue(vv)
		})
	return &MsgpackCodecFactory {
		cfg: cfg,
	}
}

func (cf *MsgpackCodecFactory) NewDecoder(c net.Conn) Decoder {
	r := bufio.NewReader(c)
	return &MsgpackDecoder{
		*msgpack.NewDecoder(r), r, 
	}
}

func (cf *MsgpackCodecFactory) NewEncoder(c net.Conn) Encoder {
	w := bufio.NewWriter(c)
	return &MsgpackEncoder{
		*msgpack.NewEncoder(w), w,
	}
}


//implementation of DecoderFactory/EncoderFactory, by json.
type JsonDecoder struct {
	json.Decoder
	reader *bufio.Reader
}

type JsonEncoder struct {
	json.Encoder
	writer *bufio.Writer
}

func (e *JsonEncoder) Flush() error {
	return e.writer.Flush()
}

type JsonCodecFactory struct {
	cfg func (interface{})
}

func NewJsonCodecFactory(cfg func (interface{})) *JsonCodecFactory {
	return &JsonCodecFactory {
		cfg: cfg,
	}
}

func (cf *JsonCodecFactory) NewDecoder(c net.Conn) Decoder {
	r := bufio.NewReader(c)
	return &JsonDecoder{
		*json.NewDecoder(r), r, 
	}
}

func (cf *JsonCodecFactory) NewEncoder(c net.Conn) Encoder {
	w := bufio.NewWriter(c)
	return &JsonEncoder{
		*json.NewEncoder(w), w,
	}
}
