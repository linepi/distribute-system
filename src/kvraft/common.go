package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Id       int64
	ServerId int
	RpcId    int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	Id       int64
	ServerId int
	RpcId    int32
}

type GetReply struct {
	Err   Err
	Value string
}

type FinishArgs struct {
	Ids []int64
}

type FinishReply struct {
	Err Err
}
