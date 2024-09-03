package raft

import (
	"bytes"
	"testing"
)

func TestTimeout(t *testing.T) {

}

func TestCoder(t *testing.T) {
	p := Persistence{}
	stateByte := toByte(p.state())
	var state PersistentState
	fromByte(stateByte, &state)
	if !bytes.Equal(stateByte, toByte(state)) {
		t.Failed()
	}
}
