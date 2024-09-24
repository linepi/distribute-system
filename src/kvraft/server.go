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
	GetWaitTimeout       = raft.Timeout{Fixed: 5000}
	PutAppendWaitTimeout = raft.Timeout{Fixed: 5000}
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

type DataEntry struct {
	LastReqId int64
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data    map[string]DataEntry // actual state machine k/v data
	request map[int64]Request    // map request id to log index
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

	logPrefix := fmt.Sprintf("[s%v][req%v][rpc%v]", kv.me, args.Id&0xffffffff, args.RpcId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
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
		var op Op
		op = Op{OpGet, args.Key, "", args.Id}
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
	logPrefix := fmt.Sprintf("[s%v][req%v][rpc%v]", kv.me, args.Id&0xffffffff, args.RpcId)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		Log.Printf("%v GetState return is not leader\n", logPrefix)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.request[args.Id]
	if ok && kv.request[args.Id].StateMachineUpdated {
		reply.Err = OK
		return
	} else {
		var op Op
		op = Op{optype, args.Key, args.Value, args.Id}
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

func (kv *KVServer) Finish(args *FinishArgs, reply *FinishReply) {
	if kv.killed() {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, id := range args.Ids {
		delete(kv.request, id)
	}
	reply.Err = OK
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
		if msg.CommandValid {
			var op Op
			op = msg.Command.(Op)
			Log.Printf("[s%v] applied op {type: %v, key: \"%v\", value: \"%v\"}\n",
				kv.me, optype2str(op.Type), op.Key, op.Val)
			req, ok := kv.request[op.Id]
			if !ok || !req.StateMachineUpdated {
				if !ok {
					// this will happen if the op request happend in other servers
					req = Request{false, false, "", nil}
				}

				entry, exist := kv.data[op.Key]
				if op.Type == OpPut {
					kv.data[op.Key] = DataEntry{op.Id, op.Val}
					req.StateMachineUpdated = true
				} else if op.Type == OpAppend {
					if exist && entry.LastReqId != op.Id {
						kv.data[op.Key] = DataEntry{op.Id, entry.Value + op.Val}
					} else {
						kv.data[op.Key] = DataEntry{op.Id, op.Val}
					}
					req.StateMachineUpdated = true
				} else if op.Type == OpGet {
					if exist {
						req.HasValue = true
						req.Value = entry.Value
					} else {
						req.HasValue = false
					}
					req.StateMachineUpdated = true
				} else {
					log.Fatalf("Not expect")
				}
				kv.request[op.Id] = req
				// if this server get this request, wakeup all waiting goroutine
				if kv.request[op.Id].Done != nil {
					close(kv.request[op.Id].Done)
				}
			}

			// at least 32 log should snapshot
			currentIndex := msg.CommandIndex
			snapshotLastIndex := kv.rf.SnapshotLastIndex()
			if kv.maxraftstate != -1 && kv.rf.CurrentStateSize() > int64(kv.maxraftstate) &&
				currentIndex-snapshotLastIndex > 32 {
				Log.Printf("snapshot when state size is %v(maxstatesize %v)\n",
					kv.rf.CurrentStateSize(), kv.maxraftstate)
				kv.snapshot(currentIndex)
			}
		} else if msg.SnapshotValid {
			Log.Printf("[s%v] applied SNAPSHOT {index: %v, term: %v}\n",
				kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			kv.fromSnapshot(msg.Snapshot)
		} else {
			Panic()
		}

		kv.mu.Unlock()
	}
}

type SnapshotData struct {
	Data    map[string]DataEntry
	Request map[int64]Request // map request id to log index
}

// need lock
func (kv *KVServer) snapshot(index int) {
	kv.rf.Snapshot(index, raft.ToByte(SnapshotData{kv.data, kv.request}))
}

func (kv *KVServer) fromSnapshot(snapshot []byte) {
	AssertNoReason(len(snapshot) > 0)
	var sd SnapshotData
	raft.FromByte(snapshot, &sd)
	kv.data = sd.Data
	kv.request = sd.Request
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

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.fromSnapshot(snapshot)
	} else {
		kv.data = make(map[string]DataEntry)
		kv.request = make(map[int64]Request)
	}

	go kv.applyMsgReceiver()

	return kv
}
