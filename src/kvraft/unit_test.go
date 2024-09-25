package kvraft

import (
	"6.5840/raft"
	"fmt"
	"testing"
	"time"
)

func channelWork(c chan struct{}) {
	go func() {
		c <- struct{}{}
	}()
	select {
	case <-c:
		fmt.Println("work")
	case <-time.After(1 * time.Second):
		fmt.Println("timeout")
	}
}

func TestCoder(t *testing.T) {
	req := Request{
		StateMachineUpdated: false,
		HasValue:            false,
		Value:               "",
		Done:                make(chan struct{}, 1),
	}

	bytes := raft.ToByte(req)
	var req2 Request
	raft.FromByte(bytes, &req2)
	if req2.Done != nil {
		channelWork(req2.Done)
	}
	channelWork(req.Done)
}
