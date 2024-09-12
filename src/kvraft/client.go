package kvraft

import (
	"6.5840/labrpc"
	"6.5840/raft"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

var requestId atomic.Int32
var clerkId atomic.Int32

var (
	NoLeaderTolerateTime = raft.Timeout{Fixed: 1000}
	GetRpcInterval       = raft.Timeout{Fixed: 1000}
	PutRpcInterval       = raft.Timeout{Fixed: 1000}
	AppendRpcInterval    = raft.Timeout{Fixed: 1000}
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	id          int32
	leaderIndex atomic.Int32 // last thought leader index in servers
}

func nrand() int64 {
	maxval := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, maxval)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = clerkId.Add(1)
	return ck
}

// return ok
func (ck *Clerk) onGetReply(args *GetArgs, reply *GetReply, buffer chan *GetReply) {
	Log.Printf("[c%v][r%v] Get(%v) = \"%v\" from s%v, Err: %v\n",
		ck.id, args.Id, args.Key, reply.Value, args.ServerId, reply.Err)

	if reply.Err == OK || reply.Err == ErrNoKey {
		ck.leaderIndex.Store(int32(args.ServerId))
		buffer <- reply
	}
}

func (ck *Clerk) onWriteReply(
	op string, args *PutAppendArgs, reply *PutAppendReply, buffer chan *PutAppendReply) {
	if op == "Put" {
		Log.Printf("[c%v][r%v] Put to s%v: key:\"%s\" value:\"%s\", Err: %v\n",
			ck.id, args.Id, args.ServerId, args.Key, args.Value, reply.Err)
	} else {
		Log.Printf("[c%v][r%v] Append to s%v: key:\"%s\" value:\"%s\", Err: %v\n",
			ck.id, args.Id, args.ServerId, args.Key, args.Value, reply.Err)
	}

	if reply.Err == OK {
		ck.leaderIndex.Store(int32(args.ServerId))
		buffer <- reply
	}
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	requestIdGet := int64(requestId.Add(1)) | int64(ck.id)<<32
	replyBuffer := make(chan *GetReply, 1)
	meth := "KVServer.Get"

	stop := atomic.Bool{}
	stop.Store(false)

	lastLeaderIndex := ck.leaderIndex.Load()
	go func() {
		for !stop.Load() {
			go func() {
				args := GetArgs{key, requestIdGet, int(lastLeaderIndex)}
				reply := GetReply{}
				ok := ck.servers[lastLeaderIndex].Call(meth, &args, &reply)
				if ok {
					ck.onGetReply(&args, &reply, replyBuffer)
				}
			}()
			time.Sleep(GetRpcInterval.New())
		}
	}()

	select {
	case reply := <-replyBuffer:
		AssertNoReason(reply.Err == ErrNoKey || reply.Err == OK)
		stop.Store(true)
		return reply.Value
	case <-time.After(NoLeaderTolerateTime.New()):
		for i := 0; i < len(ck.servers); i++ {
			go func(i int) {
				for !stop.Load() {
					go func() {
						args := GetArgs{key, requestIdGet, i}
						reply := GetReply{}
						ok := ck.servers[i].Call(meth, &args, &reply)
						if ok {
							ck.onGetReply(&args, &reply, replyBuffer)
						}
					}()
					time.Sleep(GetRpcInterval.New())
				}
			}(i)
		}
		select {
		case reply := <-replyBuffer:
			AssertNoReason(reply.Err == ErrNoKey || reply.Err == OK)
			stop.Store(true)
			return reply.Value
		}
	}
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestIdWrite := int64(requestId.Add(1)) | int64(ck.id)<<32
	Log.Printf("[c%v][r%v] call PutAppend(\"%v\", \"%v\", %v)\n", ck.id, requestIdWrite, key, value, op)
	replyBuffer := make(chan *PutAppendReply, 1)
	meth := "KVServer." + op
	var rpcInterval time.Duration
	if op == "Put" {
		rpcInterval = PutRpcInterval.New()
	} else {
		rpcInterval = AppendRpcInterval.New()
	}

	stop := atomic.Bool{}
	stop.Store(false)

	lastLeaderIndex := ck.leaderIndex.Load()
	go func() {
		for !stop.Load() {
			go func() {
				args := PutAppendArgs{key, value, requestIdWrite, int(lastLeaderIndex)}
				reply := PutAppendReply{}
				ok := ck.servers[lastLeaderIndex].Call(meth, &args, &reply)
				if ok {
					ck.onWriteReply(op, &args, &reply, replyBuffer)
				} else {
					Log.Printf("[c%v][r%v] rpc failed to s%v\n", ck.id, requestIdWrite, int(lastLeaderIndex))
				}
			}()
			time.Sleep(rpcInterval)
		}
	}()

	select {
	case reply := <-replyBuffer:
		AssertNoReason(reply.Err == OK)
		stop.Store(true)
	case <-time.After(NoLeaderTolerateTime.New()):
		for i := 0; i < len(ck.servers); i++ {
			if i == int(lastLeaderIndex) {
				continue
			}
			go func(i int) {
				for !stop.Load() {
					go func() {
						args := PutAppendArgs{key, value, requestIdWrite, i}
						reply := PutAppendReply{}
						ok := ck.servers[i].Call(meth, &args, &reply)
						if ok {
							ck.onWriteReply(op, &args, &reply, replyBuffer)
						} else {
							Log.Printf("[c%v][r%v] rpc failed to s%v\n", ck.id, requestIdWrite, i)
						}
					}()
					time.Sleep(rpcInterval)
				}
			}(i)
		}
		select {
		case reply := <-replyBuffer:
			AssertNoReason(reply.Err == OK)
			stop.Store(true)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
