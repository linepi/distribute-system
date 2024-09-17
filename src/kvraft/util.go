package kvraft

import (
	"6.5840/raft"
)

var Log = raft.Log
var Assert = raft.Assert
var AssertNoReason = raft.AssertNoReason
var Panic = raft.Panic
