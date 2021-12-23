package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"sync"
	"time"
)

// just for test
type memoryStore struct {
	sync.RWMutex
	db      map[string][]byte
	rn      raft.Node
	storage *raft.MemoryStorage
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
	ctx context.Context
	m   raftpb.Message
}
type Reply struct {
}

func (s *memoryStore) recv(arg *Arg, reply *Reply) error {
	s.rn.Step(arg.ctx, arg.m)
	return nil
}

var addrs map[uint64]string

func newMemoryStore(cfg cfg.Cfg) (Store, error) {
	peersAddr(cfg)
	s := memoryStore{}
	storage := raft.NewMemoryStorage()
	s.storage = storage
	c := &raft.Config{
		ID:              0x01,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	// Set peer list to the other nodes in the cluster.
	// Note that they need to be started separately as well.
	s.rn = raft.StartNode(c, []raft.Peer{{ID: 0x01}, {ID: 0x02}, {ID: 0x03}})
	return nil, nil
}

func (s *memoryStore) Set(key []byte, value []byte) error {
	return s.Propose(SET, string(key), value)

}
func (s *memoryStore) Propose(op operateType, k string, v []byte) error {
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
func (s *memoryStore) Get(key []byte) ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	k := string(key)
	if v, ok := s.db[k]; ok {
		return v, nil
	}
	return nil, nil
}

func (s *memoryStore) Delete(key []byte) error {
	return s.Propose(DELETE, string(key), nil)
}

func (s *memoryStore) handle() {
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

func (s *memoryStore) processSnapshot(snapShot raftpb.Snapshot) {

}
func (s *memoryStore) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	s.storage.ApplySnapshot(snapshot)
	s.storage.SetHardState(hardState)
	s.storage.Append(entries)

}
func (s *memoryStore) send(messages []raftpb.Message) {

}
func (s *memoryStore) process(entry raftpb.Entry) {
	if entry.Type == raftpb.EntryNormal {
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
func peersAddr(cfg cfg.Cfg) {
	addrs = make(map[uint64]string)
	if cfg.API.Addr == "node1:8080" {
		addrs[0x02] = "node2:8080"
		addrs[0x03] = "node3:8080"
	} else if cfg.API.Addr == "node2:8080" {
		addrs[0x02] = "node1:8080"
		addrs[0x03] = "node3:8080"
	} else {
		addrs[0x02] = "node1:8080"
		addrs[0x03] = "node2:8080"
	}
}
