package yue

import (
	"os"
	"log"
	"fmt"
	"net"

	proto "github.com/umegaya/yue/proto"

	"golang.org/x/net/context"
)

//configuration of actor system
type Config struct {
	Listeners []net.Listener
	MeshServerPort int
	MeshServerNetwork string
	MeshServerCodec string
	MeshServerConnTimeoutSec int
	MeshServerPingInterval int
	DatabaseAddress string
	CertPath string
	DockerCertPath string
	HostAddress string
	DefaultApiVersion string
	Codecs map[string]CodecFactory
}

//fill missing config
func (c *Config) fill() {
	if c.MeshServerPort <= 0 {
		c.MeshServerPort = 8008
	}
	if c.MeshServerNetwork == "" {
		c.MeshServerNetwork = "tcp"
	}
	if c.MeshServerCodec == "" {
		c.MeshServerCodec = "msgpack"
	}
	if c.MeshServerConnTimeoutSec <= 0 {
		c.MeshServerConnTimeoutSec = 5
	}
	if c.MeshServerPingInterval <= 0 {
		//c.MeshServerPingInterval = 5
	}
	if c.DatabaseAddress == "" {
		log.Panicf("must specify DatabaseAddress")
	}
	if c.HostAddress == "" {
		log.Panicf("must specify HostAddress")
	}
	if c.DockerCertPath == "" {
		c.DockerCertPath = os.Getenv("DOCKER_CERT_PATH")
	}
	m := NewMsgpackCodecFactory(nil)
	j := NewJsonCodecFactory(nil)
	if c.Codecs == nil {
		c.Codecs = map[string]CodecFactory {
			"msgpack": m,
			"json": j,
		}
	} else {
		if _, ok := c.Codecs["json"]; !ok {
			c.Codecs["json"] = j
		}
		if _, ok := c.Codecs["msgpack"]; !ok {
			c.Codecs["msgpack"] = m
		}
	}
	codec := c.MeshServerCodec
	if _, ok := c.Codecs[codec]; !ok {
		log.Panicf("codec not exists: %v", codec)
	}
}



type UUID proto.UUID

//module global vars
var _server *server = newserver()
var _actormgr *actormgr = newactormgr()
var _database *database = newdatabase()
var _dockerctrl *dockerctrl = newdockerctrl()
var _processmgr *processmgr = newprocessmgr()

func sv() *server {
	return _server
}

func amgr() *actormgr {
	return _actormgr
}

func db() *database {
	return _database
}

func uuid() uint64 {
	return _database.node.newUUID()
}

func node() *Node {
	return _database.node
}

func dc() *dockerctrl {
	return _dockerctrl
}

func pmgr() *processmgr {
	return _processmgr
}



//API
func Init(c *Config) error {
	c.fill()
	if err := _database.init(c); err != nil {
		return err
	}
	log.Printf("init database")
	if err := _dockerctrl.init(c); err != nil {
		return err
	}
	log.Printf("init docker")
	if len(c.Listeners) > 0 {
		//TODO: implement generic frontend for actor RPC call
		//so that client can interact with actors transparently.
		return fmt.Errorf("generic actor server is not implemented... yet!")
	}
	log.Printf("start server")
	//start main listener. TODO: be configurable
	go _actormgr.start()
	go _server.listen(c)
	return nil
}

//spawn setup initialization table of given id. 
//its lazy evaluated. thus, actor object is initialized only when someone does RPC of corresponding id.
func Register(id string, conf ActorConfig, args ...interface{}) {
	_actormgr.registerConfig(id, &conf, args)
}

//send speocified RPC to given id's actor. should call in some goroutine
//last argument of args is treated as parameter which receive return value
func Call(id, method string, args ...interface{}) error {
	return RawCall(context.Background(), id, method, 1, args...)
}

//call RPC with number of return value. used for receiving multiple or no return value
func CallMR(id, method string, n_rv int, args ...interface{}) error {
	return RawCall(context.Background(), id, method, n_rv, args...)
}

//same as CallMR but can receive go's context.
func RawCall(ctx context.Context, id, method string, n_rv int, args ...interface{}) error {
	//ensure actor which is corresponding to id, is loaded.
	a, err := _actormgr.ensureLoaded(id)
	if err != nil {
		return err
	}
	return a.call(&rpcContext {
		gctx: ctx,
		n_rv: n_rv,
	}, method, args...)
}

//generate 64bit new uuid by using yue generate id facility.
//only after Init is called, it returns valid value.
func NewId() UUID {
	return UUID(uuid())
}
