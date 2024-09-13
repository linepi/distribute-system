package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpAppend = 0
	OpPut    = 1
	OpGet    = 2
)

var (
	GetWaitTimeout       = raft.Timeout{Fixed: 2000}
	PutAppendWaitTimeout = raft.Timeout{Fixed: 2000}
)

type Op struct {
	Type int
	Key  string
	Val  string
	Id   int64
}

type Request struct {
	StateMachineUpdated bool
	HasValue            bool
	Value               string
	Done                chan struct{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data    map[string]string // actual state machine k/v data
	request map[int64]Request // map request id to log index
}

func optype2str(t int) string {
	if t == OpPut {
		return "Put"
	} else if t == OpAppend {
		return "Append"
	} else {
		return "Get"
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	logPrefix := fmt.Sprintf("[s%v][req%v][rpc%v]", kv.me, args.Id&0xffffffff, args.RpcId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	_, ok := kv.request[args.Id]
	if ok && kv.request[args.Id].StateMachineUpdated {
		if kv.request[args.Id].HasValue {
			reply.Err = OK
			reply.Value = kv.request[args.Id].Value
		} else {
			reply.Err = ErrNoKey
		}
		return
	} else {
		op := Op{OpGet, args.Key, "", args.Id}
		_, _, isStartLeader := kv.rf.Start(op)
		if !isStartLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if !ok {
			kv.request[args.Id] = Request{false, false, "", make(chan struct{})}
		}
	}

	done := kv.request[args.Id].Done
	kv.mu.Unlock()
	select {
	case <-done:
		kv.mu.Lock()
		if kv.request[args.Id].HasValue {
			reply.Err = OK
			reply.Value = kv.request[args.Id].Value
		} else {
			reply.Err = ErrNoKey
		}
	case <-time.After(GetWaitTimeout.New()):
		kv.mu.Lock()
		Log.Printf("%v request timeout\n", logPrefix)
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply, optype int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	logPrefix := fmt.Sprintf("[s%v][req%v][rpc%v]", kv.me, args.Id&0xffffffff, args.RpcId)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		Log.Printf("%v GetState return is not leader\n", logPrefix)
		return
	}

	_, ok := kv.request[args.Id]
	if ok && kv.request[args.Id].StateMachineUpdated {
		reply.Err = OK
		return
	} else {
		op := Op{optype, args.Key, args.Value, args.Id}
		_, _, isStartLeader := kv.rf.Start(op)
		if !isStartLeader {
			reply.Err = ErrWrongLeader
			Log.Printf("%v start's return is not leader\n", logPrefix)
			return
		}
		if !ok {
			kv.request[args.Id] = Request{false, false, "", make(chan struct{})}
		}
	}

	Log.Printf("%v start wait\n", logPrefix)

	done := kv.request[args.Id].Done
	kv.mu.Unlock()
	select {
	case <-done:
		kv.mu.Lock()
		reply.Err = OK
	case <-time.After(PutAppendWaitTimeout.New()):
		kv.mu.Lock()
		Log.Printf("%v request timeout\n", logPrefix)
		reply.Err = ErrTimeout
	}

	Log.Printf("%v end wait\n", logPrefix)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		return
	}
	kv.PutAppend(args, reply, OpPut)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		return
	}
	kv.PutAppend(args, reply, OpAppend)
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyMsgReceiver() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		kv.mu.Lock()
		var op Op
		op = msg.Command.(Op)
		if msg.CommandValid {
			Log.Printf("[s%v] applied op {type: %v, key: \"%v\", value: \"%v\"}\n",
				kv.me, optype2str(op.Type), op.Key, op.Val)
		} else if msg.SnapshotValid {
			Log.Printf("[s%v] applied SNAPSHOT {index: %v, term: %v, data: %v}\n",
				kv.me, msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
		} else {
			Panic()
		}

		req, ok := kv.request[op.Id]
		if !ok || !req.StateMachineUpdated {
			if !ok {
				req = Request{true, false, "", nil}
			} else {
				req.StateMachineUpdated = true
			}
			if op.Type == OpPut {
				kv.data[op.Key] = op.Val
			} else if op.Type == OpAppend {
				val, exist := kv.data[op.Key]
				if exist {
					kv.data[op.Key] = val + op.Val
				} else {
					kv.data[op.Key] = op.Val
				}
			} else if op.Type == OpGet {
				val, exist := kv.data[op.Key]
				if exist {
					req.HasValue = true
					req.Value = val
				} else {
					req.HasValue = false
				}
			} else {
				log.Fatalf("Not expect")
			}
			kv.request[op.Id] = req
			// if this server get this request, wakeup all waiting goroutine
			if kv.request[op.Id].Done != nil {
				close(kv.request[op.Id].Done)
			}
		}
		kv.mu.Unlock()
	}
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.request = make(map[int64]Request)

	go kv.applyMsgReceiver()

	return kv
}
