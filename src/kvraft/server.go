package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpAppend = 0
	OpPut    = 1
)

var (
	PutWaitInterval    = raft.Timeout{Fixed: 5}
	AppendWaitInterval = raft.Timeout{Fixed: 5}
	WriteTimeout       = raft.Timeout{Fixed: 2000}
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type int
	Key  string
	Val  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	opmap   map[int]Op        // map log index to operation
	data    map[string]string // actual state machine k/v data
	request map[int64]int     // map request id to log index
}

func optype2str(t int) string {
	if t == OpPut {
		return "Put"
	} else {
		return "Append"
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	val, ok := kv.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Value = val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply, optype int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var index int
	value, ok := kv.request[args.Id]
	if ok {
		index = value
	} else {
		op := Op{optype, args.Key, args.Value}
		idx, _, isStartLeader := kv.rf.Start(op)
		if !isStartLeader {
			reply.Err = ErrWrongLeader
			return
		}
		index = idx
		kv.request[args.Id] = index
	}

	start := time.Now()
	for {
		_, ok = kv.opmap[index]
		if ok {
			return
		}
		kv.mu.Unlock()
		time.Sleep(PutWaitInterval.New())
		kv.mu.Lock()

		_, isLeader = kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		if time.Now().Sub(start) > WriteTimeout.New() {
			reply.Err = ErrTimeout
			return
		}
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply, OpPut)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
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
		kv.mu.Lock()
		var op Op
		op = msg.Command.(Op)
		kv.opmap[msg.CommandIndex] = op

		if msg.CommandValid {
			Log.Printf("[s%v] applied op {type: %v, key: \"%v\", value: \"%v\"}\n",
				kv.me, optype2str(op.Type), op.Key, op.Val)
		} else if msg.SnapshotValid {
			Log.Printf("[s%v] applied SNAPSHOT {index: %v, term: %v, data: %v}\n",
				kv.me, msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
		} else {
			Panic()
		}

		if op.Type == OpPut {
			kv.data[op.Key] = op.Val
		} else if op.Type == OpAppend {
			val, ok := kv.data[op.Key]
			if ok {
				kv.data[op.Key] = val + op.Val
			} else {
				kv.data[op.Key] = op.Val
			}
		} else {
			log.Fatalf("Not expect")
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.opmap = make(map[int]Op)
	kv.data = make(map[string]string)
	kv.request = make(map[int64]int)

	go kv.applyMsgReceiver()

	return kv
}
