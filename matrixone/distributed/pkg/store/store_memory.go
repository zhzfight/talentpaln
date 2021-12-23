package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

// just for test
type MemoryStore struct {
	db      map[string][]byte
	rn      raft.Node
	storage *raft.MemoryStorage
	id      uint64
}

type operateType int8

const (
	SET    operateType = 0
	DELETE operateType = 1
)

type operate struct {
	opType operateType
	key    string
	value  []byte
}

func recover() (Store, error) {

	return nil, nil
}

type Arg struct {
	M raftpb.Message
}
type Reply struct {
	Success bool
}

func (s *MemoryStore) Receive(arg *Arg, reply *Reply) error {
	log.Print("get message ", arg.M)
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	s.rn.Step(ctx, arg.M)

	return nil
}

var addrs map[uint64]string

func newMemoryStore(cfg cfg.Cfg) (Store, error) {
	peersAddr()
	s := new(MemoryStore)
	storage := raft.NewMemoryStorage()
	s.storage = storage
	a := cfg.API.Addr[4]
	i := a - 48
	s.id = uint64(i)
	c := &raft.Config{
		ID:              s.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	// Set peer list to the other nodes in the cluster.
	// Note that they need to be started separately as well.
	rpc.Register(s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8081")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	s.rn = raft.StartNode(c, []raft.Peer{{ID: 0x01}, {ID: 0x02}, {ID: 0x03}})
	go s.handle()
	return nil, nil
}

func (s *MemoryStore) Set(key []byte, value []byte) error {
	return s.propose(SET, string(key), value)

}
func (s *MemoryStore) propose(op operateType, k string, v []byte) error {
	buf := new(bytes.Buffer)
	e := gob.NewEncoder(buf)
	e.Encode(operate{op, k, v})
	data := buf.Bytes()
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	s.rn.Propose(ctx, data)
	<-ctx.Done()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil

}
func (s *MemoryStore) Get(key []byte) ([]byte, error) {

	k := string(key)
	if v, ok := s.db[k]; ok {
		return v, nil
	}
	return nil, nil
}

func (s *MemoryStore) Delete(key []byte) error {
	return s.propose(DELETE, string(key), nil)
}

func (s *MemoryStore) handle() {
	ticker := time.NewTicker(100 * time.Millisecond).C
	for {
		select {
		case <-ticker:
			s.rn.Tick()
		case rd := <-s.rn.Ready():
			s.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			s.send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				s.processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				s.process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					s.rn.ApplyConfChange(cc)
				}
			}
			s.rn.Advance()
		}
	}
}

func (s *MemoryStore) processSnapshot(snapShot raftpb.Snapshot) {

}
func (s *MemoryStore) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	s.storage.ApplySnapshot(snapshot)
	s.storage.SetHardState(hardState)
	s.storage.Append(entries)

}
func (s *MemoryStore) send(messages []raftpb.Message) {
	for _, msg := range messages {
		if msg.To == s.id {
			continue
		}
		go func() {
			log.Print("call to ", msg.To, " ", addrs[msg.To])
			client, err := rpc.DialHTTP("tcp", addrs[msg.To])
			if err != nil {
				log.Fatal(err)
			}
			reply := Reply{}
			err1 := client.Call("MemoryStore.Receive", Arg{M: msg}, &reply)
			if err1 != nil {
				log.Fatal(err1)
			}

		}()

	}
}
func (s *MemoryStore) process(entry raftpb.Entry) {
	if entry.Type == raftpb.EntryNormal {
		if len(entry.Data) != 0 {
			r := bytes.NewBuffer(entry.Data)
			d := gob.NewDecoder(r)
			var op operate
			if d.Decode(&op) != nil {
				log.Fatal("decode entry fail")
			}
			if op.opType == SET {
				s.db[op.key] = op.value
			} else if op.opType == DELETE {
				delete(s.db, op.key)
			}
		}

	}
}
func peersAddr() {
	addrs = make(map[uint64]string)
	addrs[0x01] = "node1:8081"
	addrs[0x02] = "node2:8081"
	addrs[0x03] = "node3:8081"

}
