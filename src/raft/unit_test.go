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
	snapshotByte := toByte(p.snapshot())
	var state PersistentState
	fromByte(stateByte, &state)
	var snapshot PersistentSnapShot
	fromByte(snapshotByte, &snapshot)
	if !bytes.Equal(stateByte, toByte(state)) {
		t.Failed()
	}
	if !bytes.Equal(snapshotByte, toByte(snapshot)) {
		t.Failed()
	}
}
