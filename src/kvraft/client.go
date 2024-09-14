package kvraft

import (
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

var clerkId atomic.Int32

var (
	NoLeaderTolerateTime   = raft.Timeout{Fixed: 1000}
	GetRpcInterval         = raft.Timeout{Fixed: 3000}
	PutRpcInterval         = raft.Timeout{Fixed: 3000}
	AppendRpcInterval      = raft.Timeout{Fixed: 3000}
	RequestIdClearInterval = raft.Timeout{Fixed: 2000}
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	id          int32
	requestId   atomic.Int32
	rpcId       atomic.Int32
	leaderIndex atomic.Int32 // last thought leader index in servers
	requestIds  chan int64
}

func nrand() int64 {
	maxval := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, maxval)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) newReqeustId() int64 {
	return int64(ck.requestId.Add(1)) | int64(ck.id)<<32
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = clerkId.Add(1)
	ck.requestIds = make(chan int64, 1000)
	go func() {
		lastTime := time.Now()
		var buffer []int64
		for reqId := range ck.requestIds {
			buffer = append(buffer, reqId)
			if time.Now().Sub(lastTime) > RequestIdClearInterval.New() {
				bufferCopy := make([]int64, len(buffer))
				copy(bufferCopy, buffer)
				for i := 0; i < len(ck.servers); i++ {
					go func(i int) {
						for !ck.sendFinishRpc(bufferCopy, i) {
						}
					}(i)
				}
				buffer = nil
				lastTime = time.Now()
			}
		}
	}()
	return ck
}

func (ck *Clerk) sendPutAppendRpc(
	op string, key *string, value *string, requestId int64, buffer chan *PutAppendReply, i int) {
	rpcid := ck.rpcId.Add(1)
	args := PutAppendArgs{*key, *value, requestId, i, rpcid}
	reply := PutAppendReply{}

	logPrefix := fmt.Sprintf("[c%v][req%v][rpc%v]", ck.id, requestId&0xffffffff, rpcid)

	Log.Printf("%v start rpc %v() to s%v\n", logPrefix, op, i)
	ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
	if ok {
		if op == "Put" {
			Log.Printf("%v Put to s%v: key:\"%s\" value:\"%s\", Err: %v\n",
				logPrefix, args.ServerId, args.Key, args.Value, reply.Err)
		} else {
			Log.Printf("%v Append to s%v: key:\"%s\" value:\"%s\", Err: %v\n",
				logPrefix, args.ServerId, args.Key, args.Value, reply.Err)
		}

		if reply.Err == OK {
			ck.leaderIndex.Store(int32(args.ServerId))
			buffer <- &reply
		}
	} else {
		Log.Printf("%v rpc %v() failed to s%v\n", logPrefix, op, i)
	}
}

func (ck *Clerk) sendGetRpc(key *string, requestId int64, buffer chan *GetReply, i int) {
	rpcid := ck.rpcId.Add(1)
	args := GetArgs{*key, requestId, i, rpcid}
	reply := GetReply{}

	logPrefix := fmt.Sprintf("[c%v][req%v][rpc%v]", ck.id, requestId&0xffffffff, rpcid)
	Log.Printf("%v start rpc Get(%v) to s%v\n", logPrefix, args.Key, i)
	ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
	if ok {
		Log.Printf("%v Get(%v) = \"%v\" from s%v, Err: %v\n",
			logPrefix, args.Key, reply.Value, i, reply.Err)

		if reply.Err == OK || reply.Err == ErrNoKey {
			ck.leaderIndex.Store(int32(args.ServerId))
			buffer <- &reply
		}
	} else {
		Log.Printf("%v rpc Get() failed to s%v\n", logPrefix, i)
	}
}

func (ck *Clerk) sendFinishRpc(requestId []int64, i int) bool {
	args := FinishArgs{requestId}
	reply := FinishReply{}
	return ck.servers[i].Call("KVServer.Finish", &args, &reply)
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
	requestIdGet := ck.newReqeustId()
	Log.Printf("[c%v][r%v] call Clerk.Get(\"%v\")\n", ck.id, requestIdGet, key)

	replyBuffer := make(chan *GetReply, len(ck.servers))
	var reply *GetReply
	done := atomic.Bool{}
	done.Store(false)
	i := int(ck.leaderIndex.Load())
	j := 0
	for ; j < len(ck.servers); j++ {
		go func(i int) {
			for !done.Load() {
				go func() {
					ck.sendGetRpc(&key, requestIdGet, replyBuffer, i)
				}()
				time.Sleep(GetRpcInterval.New())
			}
		}(i)
		if j == 0 {
			select {
			case reply = <-replyBuffer:
				done.Store(true)
				ck.requestIds <- requestIdGet
				return reply.Value
			case <-time.After(NoLeaderTolerateTime.New()):
			}
		}
		i = (i + 1) % len(ck.servers)
	}
	select {
	case reply = <-replyBuffer:
		done.Store(true)
		ck.requestIds <- requestIdGet
		return reply.Value
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
	requestIdWrite := ck.newReqeustId()
	Log.Printf("[c%v][r%v] call Clerk.PutAppend(\"%v\", \"%v\", %v)\n", ck.id, requestIdWrite, key, value, op)

	replyBuffer := make(chan *PutAppendReply, len(ck.servers))
	var rpcInterval time.Duration
	if op == "Put" {
		rpcInterval = PutRpcInterval.New()
	} else {
		rpcInterval = AppendRpcInterval.New()
	}

	var reply *PutAppendReply
	done := atomic.Bool{}
	done.Store(false)
	i := int(ck.leaderIndex.Load())
	j := 0
	for ; j < len(ck.servers); j++ {
		go func(i int) {
			for !done.Load() {
				go func() {
					ck.sendPutAppendRpc(op, &key, &value, requestIdWrite, replyBuffer, i)
				}()
				time.Sleep(rpcInterval)
			}
		}(i)
		if j == 0 {
			select {
			case reply = <-replyBuffer:
				AssertNoReason(reply.Err == OK)
				done.Store(true)
				ck.requestIds <- requestIdWrite
				return
			case <-time.After(NoLeaderTolerateTime.New()):
			}
		}
		i = (i + 1) % len(ck.servers)
	}
	select {
	case reply = <-replyBuffer:
		AssertNoReason(reply.Err == OK)
		done.Store(true)
		ck.requestIds <- requestIdWrite
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
