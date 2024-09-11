package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

var requestId atomic.Int64
var clerkId atomic.Int64

type Clerk struct {
	servers []*labrpc.ClientEnd
	id      int64
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
	ck.id = clerkId.Load()
	clerkId.Add(1)
	// You'll have to add code here.
	return ck
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
	args := GetArgs{key}
	replyBuffer := make(chan GetReply, 1)
	stop := atomic.Bool{}
	stop.Store(false)
	for i := 0; i < len(ck.servers); i++ {
		go func(i int) {
			for !stop.Load() {
				reply := GetReply{}
				ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
				if ok && reply.Err != ErrWrongLeader {
					replyBuffer <- reply
					Log.Printf("[c%v] Get(%v) = \"%v\" from s%v, Err: %v\n",
						ck.id, key, reply.Value, i, reply.Err)
					break
				}
			}
		}(i)
	}
	for reply := range replyBuffer {
		if reply.Err == ErrNoKey {
			stop.Store(true)
			break
		} else {
			return reply.Value
		}
	}
	return ""
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
	Log.Printf("[c%v] call PutAppend(\"%v\", \"%v\", %v)\n", ck.id, key, value, op)
	requestId.Add(1)
	arg := PutAppendArgs{key, value, requestId.Load()}
	replyBuffer := make(chan PutAppendReply, 1)
	stop := atomic.Bool{}
	stop.Store(false)
	for i := 0; i < len(ck.servers); i++ {
		go func(i int) {
			for !stop.Load() {
				reply := PutAppendReply{}
				ok := ck.servers[i].Call("KVServer."+op, &arg, &reply)
				if ok && reply.Err == "" {
					replyBuffer <- reply
					if op == "Put" {
						Log.Printf("[c%v][%v] Put to s%v: key:\"%s\" value:\"%s\"\n", ck.id, arg.Id, i, key, value)
					} else {
						Log.Printf("[c%v][%v] Append to s%v: key:\"%s\" value:\"%s\"\n", ck.id, arg.Id, i, key, value)
					}
					break
				}
			}
		}(i)
	}
	for range replyBuffer {
		stop.Store(true)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
