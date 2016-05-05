package yue

import (
	"log"
	"fmt"
	"net"
	"time"
	"strings"

	proto "github.com/umegaya/yue/proto"

	"github.com/docker/engine-api/types"
	"github.com/docker/engine-api/types/container"

	"golang.org/x/net/context"
)

//
// containerExecuter
//
//Executer interface implemented by docker container. it lived in seperate process as which runs yue.
//entire system be more stable, but additional overhead.
type containerExecuter struct {
	pid proto.ProcessId
	cid string
	addr string
	codec string
	conn *conn
	shutdown bool
}

//Call implements Executer interface
func (cp containerExecuter) Call(method string, args ...interface{}) (interface {}, error) {
	if cp.conn == nil {
		c, err := net.Dial("tcp", cp.addr)
		if err != nil {
			if !cp.shutdown {
				panic(err) //panic to restart process
			}
			return nil, err
		}
		cp.conn = newconn(c, connConfig{
			codec: cp.codec,
		})
		cp.conn.Run(&cp)
	}
	var r interface{}
	if err := cp.conn.Request(&rpcContext{}, sv().NewMsgId(), cp.pid, method, append(args, r)); err != nil {
		return nil, err
	} else {
		return r, nil
	}
}
//process request. implements connExecuteror interface
func (cp *containerExecuter) ProcessRequest(r *request, c peer) {
	log.Printf("container actor never dispatch request")
}

//process notify. implements connExecuteror interface
func (cp *containerExecuter) ProcessNotify(n *notify) {
	log.Printf("container actor never dispatch notify")
}

//create encoder for specified codec. implements connProcessor interface
func (cp *containerExecuter) NewDecoder(codec string, c net.Conn) Decoder {
	return sv().NewDecoder(codec, c)
}

//create decoder for specified codec. implements connProcessor interface
func (cp *containerExecuter) NewEncoder(codec string, c net.Conn) Encoder {
	return sv().NewEncoder(codec, c)
}

func (cp *containerExecuter) NewMsgId() proto.MsgId {
	return sv().NewMsgId()
}

	
//connection closed. implements connExecuteror interface
func (cp *containerExecuter) Exit(c peer) {
	log.Printf("container actor shutdown")
	conn := cp.conn
	cp.conn = nil 
	conn.Close()
}

//
// ContainerExecuterFactory
//
//represents configuration for creating actor by container
//it is compatible for docker remote API (through docker/engine-api)
type ContainerExecuterFactory struct {
	Config     container.Config     `json:"config"`
	HostConfig container.HostConfig `json:"host_config"`
}

func (cf ContainerExecuterFactory) fillEnv(filler map[string]func(p string)string) {
	c := cf.Config
	for pattern, fill := range filler {
		found := false
		for _, elem := range c.Env {
			if strings.HasPrefix(elem, pattern) {
				found = true
			}
		}
		if !found {
			c.Env = append(c.Env, fill(pattern))
		}
	}
}

func (cf ContainerExecuterFactory) Create(pid uint64, args ...interface{}) (Executer, error) {
	c := cf.Config
	client := dc().myclient
	//supplement necessary environment variable for container (if not exists)
	cf.fillEnv(map[string]func(string)string {
		"YUE_DB_ADDR": func (p string) string {
			return fmt.Sprintf("%s=%s", p, sv().config.DatabaseAddress)
		}, 
		"YUE_INIT_ARGS": func (p string) string {
			sargs := make([]string, len(args))
			for idx, a := range args {
				sargs[idx] = a.(string)
			}
			return fmt.Sprintf("%s=%s", p, strings.Join(sargs, " "))
		},
	})
	ctx, _ := context.WithTimeout(context.TODO(), 300 * time.Second)
	ct, err := client.ContainerCreate(ctx, &c, &cf.HostConfig, nil, fmt.Sprintf("yue-pid-%v", pid))
	if err != nil {
		return nil, err
	}
	if err := client.ContainerStart(ctx, ct.ID); err != nil {
		return nil, err
	}
	json, _ := client.ContainerInspect(ctx, ct.ID)

	return &containerExecuter {
		pid: proto.ProcessId(pid),
		cid: ct.ID,
		addr: json.NetworkSettings.IPAddress,
	}, nil
}

func (cf ContainerExecuterFactory) Destroy(e Executer) {
	if p, ok := e.(containerExecuter); ok {
		client := dc().myclient
		p.shutdown = true //set flag to avoid restarting
		log.Printf("stop and cleanup container:", p.pid)
		ctx, _ := context.WithTimeout(context.TODO(), 300 * time.Second)
		client.ContainerKill(ctx, p.cid, "")
		client.ContainerRemove(ctx, types.ContainerRemoveOptions{
			ContainerID: p.cid,
			RemoveVolumes: true,
			Force:         true,			
		})
		//it eventually closes connection between local ContainerExecuter and Container, 
		//then ContainerExecuter.Exit called. so we never touch p.conn
	} else {
		log.Fatalf("invalid executer passed: %v", e)
	}
}

