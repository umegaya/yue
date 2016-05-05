package yue

import (
	"log"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	proto "github.com/umegaya/yue/proto"
)

// error
var ConnCreationOnGoing error = fmt.Errorf("connection creation on going")


//
// svconn
//
type svconn struct {
	conn
	nid proto.NodeId
}

type svconnConfig struct {
	connConfig
	nid proto.NodeId
}

//helper for get ip from net.Addr
func getip(addr net.Addr) net.IP {
	if a, ok := addr.(*net.TCPAddr); ok {
		return a.IP
	} else if a, ok := addr.(*net.UDPAddr); ok {
		return a.IP
	} else if a, ok := addr.(*net.IPAddr); ok {
		return a.IP
	}
	return nil	
}

// newsvconn create mesh conn, which has node id.
func newsvconn(c net.Conn, cfg svconnConfig) (*svconn, error) {
	var err error
	nid := cfg.nid
	if nid <= 0 {
		ip := getip(c.RemoteAddr())
		if ip == nil {
			return nil, fmt.Errorf("invalid address family: %v", c.RemoteAddr().String())
		}
		nid, err = NodeIdByAddr(ip.String())
		if err != nil {
			return nil, err
		}
	}
	return &svconn {
		*newconn(c, cfg.connConfig), nid,
	}, nil
}

//Id re-implement peer interface
func (c *svconn) Id() proto.NodeId {
	return c.nid
}

//
// server
//
type server struct {
	pmap map[proto.NodeId]peer
	pmtx sync.RWMutex
	nidLock map[proto.NodeId]bool
	nlmtx sync.RWMutex
	join chan peer
	leave chan peer
	seed int32
	config *Config
}

//NewServer creates new server instance
func newserver() *server {
	return &server {
		pmap: make(map[proto.NodeId]peer),
		pmtx: sync.RWMutex{},
		nidLock: make(map[proto.NodeId]bool),
		nlmtx: sync.RWMutex{}, 
		join: make(chan peer),
		leave: make(chan peer),
		seed: 0,
	}
}

//run runs server event loop
func (sv *server) listen(cfg *Config) {
	go sv.process()
	var (
		c net.Conn
		conn *svconn
		err error
		l net.Listener
	)
	sv.config = cfg
	l, err = net.Listen(cfg.MeshServerNetwork, fmt.Sprintf(":%d", cfg.MeshServerPort))
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		// Wait for a connection.
		c, err = l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		if conn, err = newsvconn(c, svconnConfig {
			connConfig {
				codec: sv.config.MeshServerCodec,
				pingInterval: sv.config.MeshServerPingInterval,
			}, 0, 
		}); err != nil {
			log.Printf("create server conn err:%v", err)
			c.Close()
			continue
		}
		sv.accept(conn)
	}
}

//Call do rpc to specified process
func (sv *server) call(ctx *rpcContext, pid proto.ProcessId, method string, args ...interface {}) *ActorError {
	p, err := sv.find(ctx, pid)
	if err != nil {
		return newerr(ActorRuntimeError, err.Error())
	}
	return p.Request(ctx, sv.NewMsgId(), pid, method, args)
}

//notify do rpc to specified process, but never receive result
func (sv *server) notify(pid proto.ProcessId, method string, args ...interface{}) *ActorError {
	p, err := sv.find(nil, pid)
	if err != nil {
		return newerr(ActorRuntimeError, err.Error())
	}
	p.Notify(pid, method, args)	
	return nil
}

//processEvent processes event to channel
func (sv *server) process() {
	for {
		select {
		case p := <-sv.leave:
			log.Printf("close %s(%v)", p.Addr().String(), p.Id())
			sv.pmtx.Lock()
			delete(sv.pmap, p.Id())
			sv.pmtx.Unlock()
			p.Close()
		}
	}
}

//startConn starts connection processing
func (sv *server) accept(p peer) {
	log.Printf("accept %s(%v)", p.Addr().String(), p.Id())
	sv.pmtx.Lock()
	op, ok := sv.pmap[p.Id()]
	if !ok {
		sv.pmap[p.Id()] = p
	} else {
		log.Printf("nid %v already filled by %v", p.Id(), op.Addr().String())
	}
	sv.pmtx.Unlock()
	go p.Run(sv)	
}

//MsgId generates new msgid
func (sv *server) NewMsgId() proto.MsgId {
	if atomic.CompareAndSwapInt32(&sv.seed, 20 * 1000 * 1000, 1) {
		return proto.MsgId(1)
	} else {
		return proto.MsgId(atomic.AddInt32(&sv.seed, 1))
	}
}

//Find finds or creates connection for pid
func (sv *server) find(ctx *rpcContext, pid proto.ProcessId) (peer, error){
	var (
		addr string
		err error
		ok bool
		p peer
		c net.Conn
	)
	nid := NodeIdByPid(pid)
RETRY:
	if p, ok = sv.peerByNodeId(nid); ok {
		return p, nil
	}
	if err = sv.nidMutex(nid, func () error {
		//below 2 api call may take long time (at most sv.config.MeshServerConnTimeoutSec), when hardware failure present.
		if addr, err = AddrByNodeId(nid); err != nil {
			return err
		}
		if c, err = net.DialTimeout(sv.config.MeshServerNetwork, 
			fmt.Sprintf("%s:%d", addr, sv.config.MeshServerPort), 
			time.Duration(sv.config.MeshServerConnTimeoutSec) * time.Second); err != nil {
			return err
		}
		if p, err = newsvconn(c, svconnConfig{
			connConfig {
				codec: sv.config.MeshServerCodec,
			}, nid,
		}); err != nil {
			c.Close()
			return err
		}
		sv.accept(p)
		return nil
	}); err == ConnCreationOnGoing {
		time.Sleep(100 * time.Millisecond)
		goto RETRY
	}
	return p, nil
}

func (sv *server) nidMutex(nid proto.NodeId, factory func () error) error {
	locked := false
	defer func () {
		if locked {
			sv.nlmtx.Lock()
			delete(sv.nidLock, nid)
			sv.nlmtx.Unlock()
		}
	}()
	sv.nlmtx.Lock()
	if _, ok := sv.nidLock[nid]; ok {
		sv.nlmtx.Unlock()
		return ConnCreationOnGoing
	}
	sv.nidLock[nid] = true
	locked = true
	sv.nlmtx.Unlock()
	return factory()
}

//connByNodeId get conn from node_id
func (sv *server) peerByNodeId(node_id proto.NodeId) (peer, bool) {
	defer sv.pmtx.RUnlock()
	sv.pmtx.RLock()
	c, ok := sv.pmap[node_id]
	return c, ok
}

//process request. implements connProcessor interface
func (sv *server) ProcessRequest(r *request, c peer) {
	//TODO: add timeout check and returns error
	waitms := 10 //10ms
RETRY:
	if p, ok := pmgr().find(r.pid); ok {
		rep, err, restart := p.call(r.method, r.args...)
		if restart {
			waitms = waitms * 2
			if waitms > 1000 {
				waitms = 1000
			}
			time.Sleep(time.Duration(waitms) * time.Millisecond)
			goto RETRY
		} else if err != nil {
			if ae, ok := err.(*ActorError); ok {
				c.Raise(r.msgid, ae)
			} else {
				c.Raise(r.msgid, newerr(ActorRuntimeError, err.Error()))
			}
		} else {
			c.Reply(r.msgid, rep)
		}
	} else {
		c.Raise(r.msgid, newerr(ActorProcessGone, r.pid))
	}
}

//process notify. implements connProcessor interface
func (sv *server) ProcessNotify(n *notify) {
	waitms := 10 //10ms
RETRY:
	if p, ok := pmgr().find(n.pid); ok {
		_, _, restart := p.call(n.method, n.args...)
		if restart {
			waitms = waitms * 2
			if waitms > 1000 {
				waitms = 1000
			}
			time.Sleep(time.Duration(waitms) * time.Millisecond)
			goto RETRY
		}
	}
}

//connection closed. implements connProcessor interface
func (sv *server) Exit(c peer) {
	log.Panicf("should not exit %v", c)
	sv.leave <- c
}

//create encoder for specified codec. implements connProcessor interface
func (sv *server) NewDecoder(codec string, c net.Conn) Decoder {
	decf, ok := sv.config.Codecs[codec]
	if !ok {
		log.Panicf("No such decoder [%s]", codec)
	}
	return decf.NewDecoder(c)
}

//create decoder for specified codec. implements connProcessor interface
func (sv *server) NewEncoder(codec string, c net.Conn) Encoder {
	encf, ok := sv.config.Codecs[codec]
	if !ok {
		log.Panicf("No such encoder [%s]", codec)
	}
	return encf.NewEncoder(c)
}
