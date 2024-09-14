package raft

import (
	"bytes"
	"testing"
)

func TestTimeout(t *testing.T) {

}

func TestCoder(t *testing.T) {
	p := Persistence{}
	stateByte := ToByte(p.state())
	var state PersistentState
	FromByte(stateByte, &state)
	if !bytes.Equal(stateByte, ToByte(state)) {
		t.Failed()
	}
}
