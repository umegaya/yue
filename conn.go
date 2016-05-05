package yue

import (
	"io"
	"log"
	"fmt"
	"net"
	"sync"
	"time"
	"container/list"

	proto "github.com/umegaya/yue/proto"

	"golang.org/x/net/context"
)

const (
	SYSTEM_REQUEST = iota
)

//
// rpcContext
//
type rpcContext struct {
	peer_id uint32 //node_id + peer connection id (aka serial) 
	deadline time.Time //absolute time to deadline
	n_rv int //number of return value
	txn interface {} //for txn call. all database operation is done in this txn.
	gctx context.Context //go cancel/timeout context. for compatibility to common go rpc implementation
}

//
// Conn
//
type conn struct {
	conn net.Conn
	send chan *payload
	recv chan *response
	closer chan struct{}
	rmap map[proto.MsgId]*request
	tick *time.Ticker
	pool *list.List
	config connConfig
	stats connStats
	wg sync.WaitGroup
}

type connConfig struct {
	codec string
	pingInterval int
}

type connStats struct {
	pingCount int
	n_send int
	latencyNS int64
}

type peer interface {
	Id() proto.NodeId
	Addr() net.Addr
	Close()
	Run(p peerProcessor)
	Request(ctx *rpcContext, msgid proto.MsgId, pid proto.ProcessId, method string, args []interface{}) *ActorError
	Notify(pid proto.ProcessId, method string, args []interface{})
	Reply(msgid proto.MsgId, args interface{})
	Raise(msgid proto.MsgId, err *ActorError)
}

type peerProcessor interface {
	NewDecoder(string, net.Conn) Decoder
	NewEncoder(string, net.Conn) Encoder
	ProcessRequest(*request, peer)
	ProcessNotify(*notify)
	Exit(peer)
	NewMsgId() proto.MsgId
}

//create Conn
func newconn(c net.Conn, cfg connConfig) *conn {
	return &conn {
		conn: c,
		send: make(chan *payload),
		recv: make(chan *response),
		closer: make(chan struct{}),
		rmap: make(map[proto.MsgId]*request),
		tick: time.NewTicker(1 * time.Second),
		pool: list.New(),
		config: cfg,
		wg: sync.WaitGroup{},
		stats: connStats {
			pingCount: cfg.pingInterval,
			n_send: 0,
		},
	}	
}

func (c *conn) Id() proto.NodeId {
	log.Panicf("override this")
	return 0
}

func (c *conn) Addr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *conn) Close() {
	c.conn.Close() //stop reader
	close(c.closer) //stop writer
}

//main loop for each connection handling
func (c *conn) Run(p peerProcessor) {
    defer p.Exit(c)
    go c.writer(p)
    c.reader(p)
}

func divideArgs(n_rv int, args []interface{}) ([]interface{}, interface{}, error) {
	if n_rv == 0 {
		return args, nil, nil
	} else if n_rv == 1 {
		if len(args) > 1 {
			return args[0:len(args) - 1], args[len(args) - 1], nil
		} else if len(args) == 1 {
			return nil, args[0], nil
		} else {
			return nil, nil, fmt.Errorf("not enough return value: need %v but %v", n_rv, len(args))
		}
	} else if len(args) > n_rv {
		recv := make([]interface{}, 0, n_rv)
		recv = append(recv, args[len(args) - n_rv:]...)
		return args[0:len(args) - n_rv], &recv, nil
	} else if len(args) == n_rv {
		return nil, &args, nil
	} else {
		return nil, nil, fmt.Errorf("not enough return value: need %v but %v", n_rv, len(args))
	}
}

//do rpc. should call in goroutine
func (c *conn) Request(ctx *rpcContext, 
	msgid proto.MsgId, pid proto.ProcessId, method string, args []interface{}) *ActorError {
	ch := c.newch()
	a, r, err := divideArgs(ctx.n_rv, args)
	if err != nil {
		return newerr(ActorRuntimeError, err)
	}
	registerRetval(msgid, r)
	c.send <- buildRequest(ctx, msgid, pid, method, a, ch)
	select {
	case resp := <- ch:
		c.pushch(ch)
		if resp.error != nil {
			return resp.error
		}
		return nil
	case <- ctx.gctx.Done():
		return newerr(ActorGoCtxError, ctx.gctx.Err())
	}
}

//do rpc but do not receive response
func (c *conn) Notify(pid proto.ProcessId, method string, args []interface{}) {
	c.send <- buildNotify(pid, method, args)
}

//used internally to send response of rpc
func (c *conn) Reply(msgid proto.MsgId, args interface{}) {
	c.send <- buildResponse(msgid, nil, args)
}

//used internally to send error of rpc
func (c *conn) Raise(msgid proto.MsgId, err *ActorError) {
	c.send <- buildResponse(msgid, err, nil)
}

//TODO: following 2 function should be evaluated again. (mutex lock cost <> malloc cost for channel)
//now I assume mutex lock has greater cost.

//newch returns empty channel for receiving rpc result
func (c *conn) newch() chan *response {
/*	defer c.mutex.Unlock()
	c.mutex.Lock()
	if c.pool.Len() > 0 {
		return c.pool.Remove(c.pool.Front).(chan *Payload)
	} */
	return make(chan *response)
}

//pushch receives channel which finishes to use, and cache it for next use.
func (c *conn) pushch(ch chan *response) {
/*	defer c.mutex.Unlock()
	c.mutex.Lock() 
	c.pool.PushFront(ch) */
}


//process response
func (c *conn) processResponse(res *response) {
	msgid := res.msgid
	if req, ok := c.rmap[msgid]; ok {
		delete(c.rmap, msgid)
	 	req.receiver <- res
	}
}

//writer used as goroutine, receive send message via send channel, write it to 
func (c *conn) writer(p peerProcessor) {
	c.wg.Add(1)
	wr := p.NewEncoder(c.config.codec, c.conn)
	for {
		select {
		case pl := <- c.send:
			log.Printf("send payload:%v", pl.String())
			if pl.kind == REQUEST {
				//request need to be wait for response
				req := pl.request()
				c.rmap[req.msgid] = req
			}
			//TODO: pack context arg and send to remote
			if err := wr.Encode(pl); err != nil {
				log.Printf("send fails %v", err)
				goto EXIT
			}
			c.stats.n_send++
			//pending flush until some number of request sent (currently 10)
			if len(c.send) <= 0 || c.stats.n_send > 10 {
				if err := wr.Flush(); err != nil {
					log.Printf("flush fails %v", err)
					goto EXIT
				}
				c.stats.n_send = 0
			}
		case res := <- c.recv:
			c.processResponse(res)
		case t := <- c.tick.C:
			for msgid, req := range c.rmap {
				ctx := req.ctx
				if ctx.deadline.UnixNano() <= 0 {
					if tmp, ok := ctx.gctx.Deadline(); ok {
						ctx.deadline = tmp
					} else {
						ctx.deadline = time.Now().Add(5 * time.Second) //default timeout: 5 sec
					}
				}
				if ctx.deadline.UnixNano() < t.UnixNano() {
					//timed out.
					delete(c.rmap, msgid)
					req.receiver <- &response{ msgid, newerr(ActorTimeout, ctx.deadline), nil }
				}
			}
			if c.config.pingInterval > 0 {
				c.stats.pingCount--
				if c.stats.pingCount <= 0 {
					go func() {
						if err := c.systemRequest(p, "ping"); err != nil {
							log.Printf("keepalive fails %v", err)
							c.Close()
							return
						}
					}()
					c.stats.pingCount = c.config.pingInterval
				}
			}
		case <- c.closer:
			goto EXIT
		}
	}
EXIT:
	c.wg.Done()
	c.wg.Wait()
	c.finalize()
}

//finalize do last cleanup of related resources. in here, only 1 goroutine will be touch connection, 
//so you can do anything without caring about race condition.
func (c *conn) finalize() {
	for msgid, req := range c.rmap {
		req.receiver <- &response{ msgid, newerr(ActorTimeout, 0), nil }
	}	
}


//reader used as goroutine, receive client message and notify it to FrontServer object
func (c *conn) reader(p peerProcessor) {
	c.wg.Add(1)
	rd := p.NewDecoder(c.config.codec, c.conn)
 	for {
 		var pl payload
 		if err := rd.Decode(&pl); err != nil {
 			if err != io.ErrShortBuffer {
 				log.Printf("reader exit: %v", err)
 				break
 			} else {
 				log.Printf("decode err: %v", err)
 			} //ErrShortBuffer means payload received on half way. (maybe...)
 		} else {
 			log.Printf("recv payload:%v", pl.String())
			switch (pl.kind) {
			case REQUEST:
				req := pl.request()
				if req.pid == 0 {
					go c.processSystemRequest(req)
				} else {
					go p.ProcessRequest(req, c)
				}
			case RESPONSE:
				c.recv <- pl.response()
			case NOTIFY:
				go p.ProcessNotify(pl.notify())
			}
		}
    }
    c.wg.Done()
}

func (c *conn) processSystemRequest(req *request) {
	switch(req.method) {
	case "ping":
		nt := (time.Now().UnixNano() - req.args[0].(int64))
		c.Reply(req.msgid, nt)
		break
	}
}

func (c *conn) systemRequest(p peerProcessor, method string) error {
	switch(method) {
	case "ping":
		if err := c.Request(&rpcContext { n_rv: 1 }, p.NewMsgId(), SYSTEM_REQUEST, method, 
			[]interface{}{time.Now().UnixNano(), &c.stats.latencyNS}); err != nil {
			return err
		}
	}
	return nil
}
