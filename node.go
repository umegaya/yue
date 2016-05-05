package yue

import (
	"log"
	"fmt"
	"time"
	"sync"

	proto "yue/proto"
)

//Account represents one user account
type Node struct {
	proto.Node
	mutex sync.Mutex `db:"-"`
	nmap map[string]*Node `db:"-"`
}

func insertNode(address string) (*Node, error) {
	var node *Node
	if err := db().txn(func (tx dbh) error {
RETRY:
		var max_id uint64
		if err := tx.SelectOne(&max_id, "select max(Id)+1 from yue.nodes"); err != nil {
			log.Printf("select max(id) fails: %v", err)
			max_id = 1
		}
		if max_id > 0x7FFF {
			return fmt.Errorf("node entry reach to limit: %v", max_id)
		}
		node = &Node {
			proto.Node {
				Id: max_id,
				Address: address,
			},
			sync.Mutex{},
			make(map[string]*Node),
		}
		id := node.newUUID() //initialize seed
		log.Printf("initial seed: %v", id)
		if err := tx.Insert(node); err != nil {
			goto RETRY
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return node, nil
}

func newnode(dbh dbh, address string) (*Node, bool, error) {
	node := &Node{}
	if err := dbh.SelectOne(node, "select * from yue.nodes where address = $1", address); err != nil {
		node, err := insertNode(address)
		if err != nil {
			return nil, false, err
		}
		return node, true, nil
	}
	return node, false, nil
}

var uniqueIDEpoch = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
const precision = uint64(10 * time.Microsecond)
const nodeIDBits = 15

func (n *Node) newUUID() uint64 {
	return n.GenUUID(time.Now())
}

func (n *Node) GenUUID(t time.Time) uint64 {
	id := uint64(t.UnixNano()-uniqueIDEpoch) / precision

	n.mutex.Lock()
	if id <= n.Seed {
		id = n.Seed + 1
	}
	n.Seed = id
	n.mutex.Unlock()
	
	id = (id << nodeIDBits) | uint64(n.Id)
	return id	
}

func (n *Node) proto() proto.Node {
	return n.Node
}

func NodeIdByPid(pid proto.ProcessId) proto.NodeId {
	return proto.NodeId(pid & ((1 << nodeIDBits) - 1))
}

func AddrByNodeId(node_id proto.NodeId) (string, error) {
	var addr string
	if err := db().SelectOne(&addr, "select address from yue.nodes where id = $1", node_id); err != nil {
		return "", err
	}
	return addr, nil
}

func NodeIdByAddr(addr string) (proto.NodeId, error) {
	var node_id proto.NodeId
	if err := db().SelectOne(&node_id, "select id from yue.nodes where address = $1", addr); err != nil {
		return 0, err
	}
	return node_id, nil
}

func UUIDAt(t time.Time) proto.UUID {
	id := uint64(t.UnixNano() - uniqueIDEpoch) / precision
	return proto.UUID(id << nodeIDBits)
}

