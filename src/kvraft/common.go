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
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	Id       int64
	ServerId int
}

type GetReply struct {
	Err   Err
	Value string
}
