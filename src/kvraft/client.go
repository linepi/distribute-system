package kvraft

import (
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

var requestId atomic.Int32
var clerkId atomic.Int32
var rpcId atomic.Int32

var (
	NoLeaderTolerateTime = raft.Timeout{Fixed: 1000}
	GetRpcInterval       = raft.Timeout{Fixed: 2000}
	PutRpcInterval       = raft.Timeout{Fixed: 2000}
	AppendRpcInterval    = raft.Timeout{Fixed: 2000}
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

func (ck *Clerk) sendPutAppendRpc(
	op string, key *string, value *string, requestId int64, buffer chan *PutAppendReply, i int) {
	rpcid := rpcId.Add(1)
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
	rpcid := rpcId.Add(1)
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

// return wake by chan
func sleepOnChan(c chan bool, duration time.Duration) bool {
	select {
	case <-time.After(GetRpcInterval.New()):
		return false
	case <-c:
		return true
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
	Log.Printf("[c%v][r%v] call Clerk.Get(\"%v\")\n", ck.id, requestIdGet, key)
	replyBuffer := make(chan *GetReply, 1)

	var wg sync.WaitGroup
	finish := make(chan bool, len(ck.servers))

	lastLeaderIndex := ck.leaderIndex.Load()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			go func() {
				ck.sendGetRpc(&key, requestIdGet, replyBuffer, int(lastLeaderIndex))
			}()
			if sleepOnChan(finish, GetRpcInterval.New()) {
				return
			}
		}
	}()

	var reply *GetReply
	select {
	case reply = <-replyBuffer:
		AssertNoReason(reply.Err == ErrNoKey || reply.Err == OK)
	case <-time.After(NoLeaderTolerateTime.New()):
		for i := 0; i < len(ck.servers); i++ {
			if i == int(lastLeaderIndex) {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for {
					go func() {
						ck.sendGetRpc(&key, requestIdGet, replyBuffer, i)
					}()
					if sleepOnChan(finish, GetRpcInterval.New()) {
						return
					}
				}
			}(i)
		}
		select {
		case reply = <-replyBuffer:
			AssertNoReason(reply.Err == ErrNoKey || reply.Err == OK)
		}
	}

	for i := 0; i < len(ck.servers); i++ {
		finish <- true
	}
	wg.Wait()
	return reply.Value
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
	Log.Printf("[c%v][r%v] call Clerk.PutAppend(\"%v\", \"%v\", %v)\n", ck.id, requestIdWrite, key, value, op)
	replyBuffer := make(chan *PutAppendReply, 1)
	var rpcInterval time.Duration
	if op == "Put" {
		rpcInterval = PutRpcInterval.New()
	} else {
		rpcInterval = AppendRpcInterval.New()
	}

	var wg sync.WaitGroup
	finish := make(chan bool, len(ck.servers))

	lastLeaderIndex := ck.leaderIndex.Load()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			go func() {
				ck.sendPutAppendRpc(op, &key, &value, requestIdWrite, replyBuffer, int(lastLeaderIndex))
			}()
			if sleepOnChan(finish, rpcInterval) {
				return
			}
		}
	}()

	select {
	case reply := <-replyBuffer:
		AssertNoReason(reply.Err == OK)
	case <-time.After(NoLeaderTolerateTime.New()):
		for i := 0; i < len(ck.servers); i++ {
			if i == int(lastLeaderIndex) {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for {
					go func() {
						ck.sendPutAppendRpc(op, &key, &value, requestIdWrite, replyBuffer, i)
					}()
					if sleepOnChan(finish, rpcInterval) {
						return
					}
				}
			}(i)
		}
		select {
		case reply := <-replyBuffer:
			AssertNoReason(reply.Err == OK)
		}
	}
	for i := 0; i < len(ck.servers); i++ {
		finish <- true
	}
	wg.Wait()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
