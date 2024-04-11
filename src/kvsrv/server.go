package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	mmap map[string]string
	request map[int64]string
}

/*
cases of calling rpc:
1. all success
2. client msg do not arrive server
3. arrived and called server rpc, but the server msg not return back to client
*/

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here.
	reply.Value = kv.mmap[args.Key]
}

func (kv *KVServer) Finish(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here.
	delete(kv.request, args.Id)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.request[args.Id]; ok {
		reply.Value = kv.request[args.Id]
		return
	}
	oldval := kv.mmap[args.Key] // store the old value
	kv.mmap[args.Key] = args.Value
	kv.request[args.Id] = oldval
	reply.Value = kv.request[args.Id]
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.request[args.Id]; ok {
		reply.Value = kv.request[args.Id]
		return
	}
	oldval := kv.mmap[args.Key] // store the old value
	kv.mmap[args.Key] = oldval + args.Value
	kv.request[args.Id] = oldval
	reply.Value = kv.request[args.Id]
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mmap = make(map[string]string)
	kv.request = make(map[int64]string)
	return kv
}
