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
	OpNone   = 3
)

var (
	GetWaitTimeout       = raft.Timeout{Fixed: 5000}
	PutAppendWaitTimeout = raft.Timeout{Fixed: 5000}
	RequestGroupSize     = int64(16)
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

type RequestGroup struct {
	Mu  sync.Mutex
	Req map[int64]Request
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data     map[string]DataEntry // actual state machine k/v data
	requests []RequestGroup       // map request id to log index
}

func (kv *KVServer) reqLock(id int64) {
	kv.requests[id%RequestGroupSize].Mu.Lock()
}

func (kv *KVServer) reqUnlock(id int64) {
	kv.requests[id%RequestGroupSize].Mu.Unlock()
}

func (kv *KVServer) reqMap(id int64) *map[int64]Request {
	return &kv.requests[id%RequestGroupSize].Req
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

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	kv.reqLock(args.Id)
	defer kv.reqUnlock(args.Id)
	reqmap := kv.reqMap(args.Id)
	req, ok := (*reqmap)[args.Id]
	if ok && req.StateMachineUpdated {
		if req.HasValue {
			reply.Err = OK
			reply.Value = req.Value
		} else {
			reply.Err = ErrNoKey
		}
		return
	} else {
		var op Op
		if ok {
			op = Op{OpNone, "", "", args.Id}
		} else {
			op = Op{OpGet, args.Key, "", args.Id}
		}
		_, _, isStartLeader := kv.rf.Start(op)
		if !isStartLeader {
			reply.Err = ErrWrongLeader
			return
		}
		if !ok {
			(*reqmap)[args.Id] = Request{false, false, "", make(chan struct{})}
		}
	}

	done := (*reqmap)[args.Id].Done
	kv.reqUnlock(args.Id)
	select {
	case <-done:
		kv.reqLock(args.Id)
		if (*reqmap)[args.Id].HasValue {
			reply.Err = OK
			reply.Value = (*reqmap)[args.Id].Value
		} else {
			reply.Err = ErrNoKey
		}
	case <-time.After(GetWaitTimeout.New()):
		kv.reqLock(args.Id)
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

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	kv.reqLock(args.Id)
	defer kv.reqUnlock(args.Id)
	reqmap := kv.reqMap(args.Id)
	req, ok := (*reqmap)[args.Id]
	if ok && req.StateMachineUpdated {
		reply.Err = OK
		return
	} else {
		var op Op
		if ok {
			op = Op{OpNone, "", "", args.Id}
		} else {
			op = Op{optype, args.Key, args.Value, args.Id}
		}
		_, _, isStartLeader := kv.rf.Start(op)
		if !isStartLeader {
			reply.Err = ErrWrongLeader
			Log.Printf("%v start's return is not leader\n", logPrefix)
			return
		}
		if !ok {
			(*reqmap)[args.Id] = Request{false, false, "", make(chan struct{})}
		}
	}

	Log.Printf("%v start wait\n", logPrefix)

	done := (*reqmap)[args.Id].Done
	kv.reqUnlock(args.Id)
	select {
	case <-done:
		kv.reqLock(args.Id)
		reply.Err = OK
	case <-time.After(PutAppendWaitTimeout.New()):
		kv.reqLock(args.Id)
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
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	idGroups := make([][]int64, RequestGroupSize)
	for i := 0; i < len(idGroups); i++ {
		idGroups[i] = make([]int64, 0)
	}
	for _, id := range args.Ids {
		idGroups[id%RequestGroupSize] = append(idGroups[id%RequestGroupSize], id)
	}
	for i := 0; i < len(idGroups); i++ {
		if len(idGroups[i]) == 0 {
			continue
		}
		kv.reqLock(idGroups[i][0])
		reqmap := kv.reqMap(idGroups[i][0])
		for _, id := range idGroups[i] {
			delete(*reqmap, id)
		}
		kv.reqUnlock(idGroups[i][0])
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
		if msg.CommandValid {
			var op Op
			op = msg.Command.(Op)
			if op.Type == OpNone {
				continue
			}
			Log.Printf("[s%v] applied op {type: %v, key: \"%v\", value: \"%v\"}\n",
				kv.me, optype2str(op.Type), op.Key, op.Val)

			kv.reqLock(op.Id)
			reqmap := kv.reqMap(op.Id)
			req, ok := (*reqmap)[op.Id]
			if !ok || !req.StateMachineUpdated {
				if !ok {
					// this will happen if the op request happend in other servers
					req = Request{false, false, "", nil}
				}

				entry, exist := kv.data[op.Key]
				if op.Type == OpPut {
					kv.data[op.Key] = DataEntry{op.Id, op.Val}
				} else if op.Type == OpAppend {
					if !exist || entry.LastReqId != op.Id {
						kv.data[op.Key] = DataEntry{op.Id, entry.Value + op.Val}
					}
				} else if op.Type == OpGet {
					if exist {
						req.HasValue = true
						req.Value = entry.Value
					} else {
						req.HasValue = false
					}
				} else {
					log.Fatalf("Not expect")
				}
				req.StateMachineUpdated = true
				(*reqmap)[op.Id] = req
				// if this server get this request, wakeup all waiting goroutine
				if ok && (*reqmap)[op.Id].Done != nil {
					close((*reqmap)[op.Id].Done)
				}
			}
			kv.reqUnlock(op.Id)

			// at least 32 log should snapshot
			currentIndex := msg.CommandIndex
			snapshotLastIndex := kv.rf.SnapshotLastIndex()
			if kv.maxraftstate != -1 && kv.rf.CurrentStateSize() > int64(kv.maxraftstate) &&
				currentIndex-snapshotLastIndex > 32 {
				Log.Printf("snapshot when state size is %v(maxstatesize %v)\n",
					kv.rf.CurrentStateSize(), kv.maxraftstate)
				kv.mu.Lock()
				kv.snapshot(currentIndex)
				kv.mu.Unlock()
			}
		} else if msg.SnapshotValid {
			Log.Printf("[s%v] applied SNAPSHOT {index: %v, term: %v}\n",
				kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			kv.mu.Lock()
			kv.fromSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		} else {
			Panic()
		}
	}
}

type SnapshotData struct {
	Data     map[string]DataEntry
	Requests []map[int64]Request // map request id to log index
}

// need lock
func (kv *KVServer) snapshot(index int) {
	snapshotData := SnapshotData{kv.data, make([]map[int64]Request, RequestGroupSize)}
	for i := 0; i < len(kv.requests); i++ {
		snapshotData.Requests[i] = kv.requests[i].Req
	}
	kv.rf.Snapshot(index, raft.ToByte(snapshotData))
}

func (kv *KVServer) fromSnapshot(snapshot []byte) {
	AssertNoReason(len(snapshot) > 0)
	var sd SnapshotData
	raft.FromByte(snapshot, &sd)
	kv.data = sd.Data
	for i := 0; i < len(sd.Requests); i++ {
		kv.requests[i].Req = sd.Requests[i]
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

	snapshot := persister.ReadSnapshot()
	kv.requests = make([]RequestGroup, RequestGroupSize)
	for i := 0; i < len(kv.requests); i++ {
		kv.requests[i] = RequestGroup{
			Mu:  sync.Mutex{},
			Req: map[int64]Request{},
		}
	}
	if len(snapshot) > 0 {
		kv.fromSnapshot(snapshot)
	} else {
		kv.data = make(map[string]DataEntry)
	}

	go kv.applyMsgReceiver()

	return kv
}
