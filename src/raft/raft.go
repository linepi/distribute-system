package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Timeout struct {
	fixed       int
	variability int
}

var (
	AppendEntryCallTimeout     = time.Duration(300) * time.Millisecond
	InstallSnapshotCallTimeout = time.Duration(300) * time.Millisecond
	RequestVoteCallTimeout     = time.Duration(300) * time.Millisecond
	AppendEntryInterval        = Timeout{80, 100}
	RequestVoteInterval        = Timeout{10, 0}
	TickerInterval             = Timeout{150, 150}
	PeerTimeoutInterval        = Timeout{450, 200}
)

func (to *Timeout) new() time.Duration {
	if to.variability == 0 {
		return time.Duration(to.fixed) * time.Millisecond
	} else {
		return time.Duration(to.fixed+(rand.Int()%to.variability)) * time.Millisecond
	}
}

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int32

const (
	UnknownState State = iota
	Follower
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"UnknownState", "Follower", "Candidate", "Leader"}[s]
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	/* leader election */
	muCom        sync.RWMutex
	currentTerm  int
	votedFor     int
	currentState State
	lastTime     time.Time // last time out
	timeoutVal   time.Duration

	/* log */
	muLog sync.RWMutex
	log   []logEntry
	// index of highest log entry known to be committed
	commitIndex int
	// index of highest log entry applied to state machine
	lastApplied int
	/* leader only, reinitialized after election */
	/*When a leader first comes to power,
	  it initializes all nextIndex values to the index just after the
	  last one in its log (11 in Figure 7). If a follower’s log is
	  inconsistent with the leader’s, the AppendEntries consistency check will fail in
	  the next AppendEntries RPC. After a rejection,
	  the leader decrements nextIndex and retries the AppendEntries RPC.
	  Eventually nextIndex will reach a point where the leader and follower logs match.
	  When this happens, AppendEntries will succeed, which removes
	  any conflicting entries in the follower’s log and appends
	  entries from the leader’s log (if any). Once AppendEntries
	  succeeds, the follower’s log is consistent with the leader’s,
	  and it will remain that way for the rest of the term.*/
	// for each server, index of the next log entry to send to that server
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	matchIndex []int

	/* snapshot */
	snapshot          []byte
	snapshotLastIndex int
	snapshotLastTerm  int
	installTemp       InstallSnapshotTemp
}

type InstallSnapshotTemp struct {
	data []byte
}

type Persistence struct {
	CurrentTerm       int
	VotedFor          int
	Log               []logEntry
	Snapshot          []byte
	SnapshotLastIndex int
	SnapshotLastTerm  int
}

type PersistentState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []logEntry
	SnapshotLastIndex int
	SnapshotLastTerm  int
}

func (rf *Raft) newPersistant() Persistence {
	rf.muLog.RLock()
	defer rf.muLog.RUnlock()
	return Persistence{
		rf.getTerm(),
		rf.getVotedFor(),
		rf.log,
		rf.snapshot,
		rf.snapshotLastIndex,
		rf.snapshotLastTerm,
	}
}

func (p *Persistence) state() PersistentState {
	return PersistentState{
		p.CurrentTerm,
		p.VotedFor,
		p.Log,
		p.SnapshotLastIndex,
		p.SnapshotLastTerm,
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
//
// NO lock is needed
func (rf *Raft) persist(persistence Persistence) {
	rf.persister.Save(
		toByte(persistence.state()),
		persistence.Snapshot,
	)
}

// restore previously persisted state.
// need muLog Lock
func (rf *Raft) readPersist(rstate []byte, rsnapshot []byte) {
	if rstate == nil || len(rstate) < 1 { // bootstrap without any state?
		return
	}
	var state PersistentState
	fromByte(rstate, &state)

	rf.setTerm(state.CurrentTerm)
	rf.setVotedFor(state.VotedFor)
	rf.muLog.Lock()
	rf.log = state.Log
	rf.snapshot = rsnapshot
	rf.snapshotLastIndex = state.SnapshotLastIndex
	rf.snapshotLastTerm = state.SnapshotLastTerm
	rf.muLog.Unlock()
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	rf.snapshot = snapshot
	rf.snapshotLastIndex = index
	rf.snapshotLastTerm = rf.log[index-1].Term
	if index == len(rf.log) {
		rf.log = []logEntry{}
	} else {
		// index-1+1 = index
		rf.log = append(rf.log[index:])
	}
	rf.persist(Persistence{
		rf.getTerm(),
		rf.getVotedFor(),
		rf.log,
		rf.snapshot,
		rf.snapshotLastIndex,
		rf.snapshotLastTerm,
	})
}

type logEntry struct {
	Cmd  interface{}
	Term int
}

func (rf *Raft) getState() State {
	rf.muCom.RLock()
	defer rf.muCom.RUnlock()
	return rf.currentState
}

func (rf *Raft) setState(s State) {
	rf.muCom.Lock()
	defer rf.muCom.Unlock()
	rf.currentState = s
}

func (rf *Raft) getTerm() int {
	rf.muCom.RLock()
	defer rf.muCom.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) setTerm(t int) {
	rf.muCom.Lock()
	defer rf.muCom.Unlock()
	rf.currentTerm = t
}

func (rf *Raft) getVotedFor() int {
	rf.muCom.RLock()
	defer rf.muCom.RUnlock()
	return rf.votedFor
}

func (rf *Raft) setVotedFor(v int) {
	rf.muCom.Lock()
	defer rf.muCom.Unlock()
	rf.votedFor = v
}

func (rf *Raft) timeout() bool {
	rf.muCom.RLock()
	defer rf.muCom.RUnlock()
	return time.Now().Sub(rf.lastTime) > rf.timeoutVal
}

func (rf *Raft) setTimeoutVal() {
	rf.muCom.Lock()
	defer rf.muCom.Unlock()
	rf.lastTime = time.Now()
	rf.timeoutVal = PeerTimeoutInterval.new()
}

func (rf *Raft) getLog(i int) logEntry {
	rf.muLog.RLock()
	defer rf.muLog.RUnlock()
	return rf.log[i-1]
}

// need muLog.RLock()
func (rf *Raft) getLastTermIndex() (int, int) {
	lastLogTerm := 0
	lastLogIndex := rf.snapshotLastIndex + len(rf.log)
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		lastLogTerm = rf.snapshotLastTerm
	}
	return lastLogTerm, lastLogIndex
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.muCom.RLock()
	defer rf.muCom.RUnlock()
	Log.Printf("GetState[%v](T: %v, State: %v)\n",
		rf.me, rf.currentTerm, rf.currentState)
	return rf.currentTerm, rf.currentState == Leader
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// return ok, isTimeout
func (rf *Raft) sendInstallSnapshot(
	peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) (bool, bool) {
	done := make(chan bool)
	ok := false
	go func() {
		ok = rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(InstallSnapshotCallTimeout):
		return false, true
	}
	return ok, false
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	rf.muLog.Lock()
	defer rf.mu.Unlock()
	defer rf.muLog.Unlock()
	reply.Term = rf.getTerm()

	Assert(args.LastIncludedIndex >= 1, "")
	Assert(args.LastIncludedTerm >= 1, "")

	if args.Term < reply.Term {
		return
	}
	rf.installTemp.data = append(rf.installTemp.data, args.Data...)
	if !args.Done {
		return
	}

	Assert(args.LastIncludedIndex > rf.snapshotLastIndex, "")
	Assert(args.LastIncludedTerm >= rf.snapshotLastTerm, "")

	rf.snapshot = rf.installTemp.data
	rf.snapshotLastIndex = args.LastIncludedIndex
	rf.snapshotLastTerm = args.LastIncludedTerm

	if rf.snapshotLastIndex <= len(rf.log) && rf.log[rf.snapshotLastIndex-1].Term == args.LastIncludedTerm {
		rf.log = append(rf.log[rf.snapshotLastIndex:])
	} else {
		rf.log = []logEntry{}
	}

	rf.persist(Persistence{
		rf.getTerm(),
		rf.getVotedFor(),
		rf.log,
		rf.snapshot,
		rf.snapshotLastIndex,
		rf.snapshotLastTerm,
	})

	rf.applyMsg(ApplyMsg{
		false, nil, 0,
		true, rf.snapshot, rf.snapshotLastTerm, rf.snapshotLastIndex,
	})
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	/* candidate's term */
	Term int
	/* candidate that is requesting vote */
	CandidateId int
	/* index of candidate’s last log entry */
	LastLogIndex int
	/* term of candidate’s last log entry */
	LastLogTerm int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	/* currentTerm, for candidate to update itself */
	Term int
	/* true means candidate received vote */
	VoteGranted bool

	// for debug
	ReceiverId int
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("{T: %v, Id: %v, LastLogIndex: %v, LastLogTerm: %v}",
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

func (args RequestVoteReply) String() string {
	return fmt.Sprintf("{T: %v, Voted: %v}", args.Term, args.VoteGranted)
}

func (rf *Raft) NewRequestVoteArgs(_ int) RequestVoteArgs {
	rf.muLog.RLock()
	lastLogTerm, lastLogIndex := rf.getLastTermIndex()
	args := RequestVoteArgs{rf.getTerm(), rf.me,
		lastLogIndex, lastLogTerm}
	rf.muLog.RUnlock()
	return args
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.getTerm()
	reply.ReceiverId = rf.me
	reply.VoteGranted = false
	if args.Term < rf.getTerm() {
		return
	} else if args.Term > rf.getTerm() {
		Log.Printf("[%v] cvt to follower by RequestVoteCall(%v)\n",
			rf.basicInfo(), args)
		rf.setTerm(args.Term) // INFO Page4, Rules for Servers
		rf.setVotedFor(-1)
		rf.persist(rf.newPersistant())
		rf.setState(Follower)
	}

	// INFO page6, "first-come-first-served" basis
	votedFor := rf.getVotedFor()
	if votedFor == -1 || votedFor == args.CandidateId {
		// only vote if candidate'log is more up-to-date than me
		/*
		   Raft determines which of two logs is more up-to-date
		   by comparing the index and term of the last entries in the
		   logs. If the logs have last entries with different terms, then
		   the log with the later term is more up-to-date. If the logs
		   end with the same term, then whichever log is longer is
		   more up-to-date.
		*/
		rf.muLog.RLock()
		lastLogTerm, lastLogIndex := rf.getLastTermIndex()
		if lastLogIndex == 0 || lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			Log.Printf("[%v] give p%v vote(newly-give: %v)\n",
				rf.basicInfo(), args.CandidateId, votedFor != args.CandidateId)
			rf.setVotedFor(args.CandidateId)
			rf.setTimeoutVal()
		}
		rf.muLog.RUnlock()
	}
}

// return (ok, isTimeout, finish)
func (rf *Raft) sendRequestVote(
	peer int, args *RequestVoteArgs, reply *RequestVoteReply, finish chan bool) (bool, bool, bool) {
	done := make(chan bool)
	ok := false
	go func() {
		ok = rf.peers[peer].Call("Raft.RequestVote", args, reply)
		done <- true
	}()
	select {
	case <-done:
	case <-finish:
		return true, false, true
	case <-time.After(RequestVoteCallTimeout):
		return false, true, false
	}
	return ok, false, false
}

type AppendEntriesArgs struct {
	/* leader's term */
	Term     int
	LeaderId int
	// int index of log entry immediately preceding new ones
	// NOTE same as lastLogIndex?
	PrevLogIndex int
	// term of prevLogIndex entry
	PrevLogTerm int
	/* log entries to store (empty for heartbeat,
	   may send more than one for efficiency) */
	Entries []logEntry
	//leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReplyFailInfo struct {
	Valid            bool
	SuggestNextIndex int
	TermIsLarge      bool
}

type AppendEntriesReply struct {
	/* currentTerm, for leader to update itself */
	Term int
	/* true if follower contained entry matching prevLogIndex and prevLogTerm */
	Success  bool
	FailInfo AppendEntriesReplyFailInfo
	// for debug
	ReceiverId int
}

type AppendEntriesWrapper struct {
	arg   AppendEntriesArgs
	reply AppendEntriesReply
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term: %v, Id: %v, PrevLogIndex: %v, PrevLogTerm: %v, "+
		"EntriesLen: %v, LeaderCommit: %v}, ",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
		len(args.Entries), args.LeaderCommit)
}

func (reply AppendEntriesReply) String() string {
	var ret string
	if reply.FailInfo.Valid {
		ret = fmt.Sprintf("{Term: %v, Suc: %v, FailInfo: %v}", reply.Term, reply.Success, reply.FailInfo)
	} else {
		ret = fmt.Sprintf("{Term: %v, Suc: %v}", reply.Term, reply.Success)
	}
	return ret
}

func (rf *Raft) NewAppendEntriesArgs(i int) AppendEntriesArgs {
	rf.muLog.RLock()
	nextLogIndex := rf.nextIndex[i] - rf.snapshotLastIndex
	Assert(nextLogIndex >= 1, "")
	var entries []logEntry
	if nextLogIndex <= len(rf.log) {
		entries = append(entries, rf.log[nextLogIndex-1:]...)
	}
	prevLogTerm := 0
	prevLogIndex := nextLogIndex - 1
	if prevLogIndex > 0 {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}
	args := AppendEntriesArgs{rf.getTerm(), rf.me,
		prevLogIndex, prevLogTerm,
		entries, rf.commitIndex}
	rf.muLog.RUnlock()
	return args
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.getTerm()
	reply.ReceiverId = rf.me
	if args.Term < rf.getTerm() {
		reply.Success = false
		return
	}

	if args.Term > rf.getTerm() {
		Log.Printf("[%v] cvt to follower by AppendEntriesCall(%v)\n",
			rf.basicInfo(), args)
		rf.setTerm(args.Term)
		rf.setVotedFor(-1)
		rf.persist(rf.newPersistant())
		rf.setState(Follower)
	}
	rf.setTimeoutVal()

	rf.muLog.Lock()
	defer rf.muLog.Unlock()
	_, lastLogIndex := rf.getLastTermIndex()

	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.FailInfo.Valid = true
		reply.FailInfo.SuggestNextIndex = lastLogIndex + 1
		return
	}

	// snapshot is not consistent with leader
	Assert(args.PrevLogIndex >= rf.snapshotLastIndex, "")
	prevTerm := 0
	prevLogIndex := args.PrevLogIndex - rf.snapshotLastIndex
	if prevLogIndex == 0 {
		prevTerm = rf.snapshotLastTerm
	} else {
		prevTerm = rf.log[prevLogIndex-1].Term
	}

	if args.PrevLogIndex >= 1 && prevTerm != args.PrevLogTerm {
		// snapshot is not consistent with leader
		Assert(prevLogIndex != 0, "")
		reply.Success = false
		if prevTerm > args.PrevLogTerm {
			initialTerm := rf.log[prevLogIndex-1].Term
			for i := prevLogIndex - 1; i >= 1; i-- {
				if rf.log[i-1].Term != initialTerm {
					reply.FailInfo.Valid = true
					reply.FailInfo.SuggestNextIndex = rf.snapshotLastIndex + i + 1
				}
			}
		} else {
			reply.FailInfo.Valid = true
			reply.FailInfo.TermIsLarge = true
		}
		return
	}

	/*
		If an existing entry conflicts with a new one (same index
		but different terms), delete the existing entry and all that
		follow it (§5.3).
		Append any new entries not already in the log.
	*/
	lastLog := rf.log
	rf.log = append(rf.log[:prevLogIndex])
	rf.log = append(rf.log, args.Entries...)
	// Log.Printf("[%v] log update from %v to %v\n", rf.basicInfo(), lastLog, rf.Log)
	Log.Printf("[%v] log length from %v to %v\n", rf.basicInfo(), len(lastLog), len(rf.log))

	rf.persist(Persistence{
		rf.getTerm(),
		rf.getVotedFor(),
		rf.log,
		rf.snapshot,
		rf.snapshotLastIndex,
		rf.snapshotLastTerm,
	})

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < args.PrevLogIndex+len(args.Entries) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				logIndex := i - rf.snapshotLastIndex
				rf.applyMsg(ApplyMsg{
					true, rf.log[logIndex-1].Cmd, i,
					false, nil, 0, 0})
			}
			rf.lastApplied = rf.commitIndex
		}
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(
	peer int, args *AppendEntriesArgs, reply *AppendEntriesReply, finish chan bool) (bool, bool, bool) {
	done := make(chan bool)
	ok := false
	go func() {
		ok = rf.peers[peer].Call("Raft.AppendEntries", args, reply)
		done <- true
	}()
	select {
	case <-done:
	case <-finish:
		return true, false, true
	case <-time.After(AppendEntryCallTimeout):
		return false, true, false
	}
	return ok, false, false
}

func (rf *Raft) applyMsg(msg ApplyMsg) {
	rf.applyCh <- msg
	Log.Printf("[%v] applied msg {valid: %v, index: %v, cmd: %v}\n",
		rf.basicInfo(), msg.CommandValid, msg.CommandIndex, cmd2str(msg.Command))
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// Return commitedIndex, currentTerm, isLeader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.muLog.Lock()
	rf.muCom.Lock()
	defer rf.muLog.Unlock()
	defer rf.muCom.Unlock()
	isLeader := rf.currentState == Leader
	if rf.killed() || !isLeader {
		return 0, 0, false
	}

	index := len(rf.log) + 1
	rf.log = append(rf.log, logEntry{command, rf.currentTerm})

	rf.persist(Persistence{
		rf.currentTerm,
		rf.votedFor,
		rf.log,
		rf.snapshot,
		rf.snapshotLastIndex,
		rf.snapshotLastTerm,
	})

	basicInfo := fmt.Sprintf("p%v-t%v-%v", rf.me, rf.currentTerm, rf.currentState)
	Log.Printf("[%v] Start(%v) -> (index:%v,term:%v,isLeader:%v)",
		basicInfo, command, index, rf.currentTerm, isLeader)
	return index, rf.currentTerm, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) election() {
	rf.setTerm(rf.getTerm() + 1)
	rf.setTimeoutVal()
	rf.setVotedFor(rf.me)
	rf.persist(rf.newPersistant())

	var wg sync.WaitGroup
	finish := make(chan bool, len(rf.peers)-1)

	// While waiting for votes, a candidate may receive an
	// AppendEntries RPC from another server claiming to be
	// leader. which may let the candidate become follower state.
	replies := make(chan struct {
		int
		RequestVoteReply
	}, len(rf.peers)-1)
	for i := 0; !rf.killed() && rf.getState() == Candidate && !rf.timeout() && i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			intervalFactor := 1
			for !rf.killed() && rf.getState() == Candidate && !rf.timeout() {
				args := rf.NewRequestVoteArgs(i)
				reply := RequestVoteReply{}
				Log.Printf("[%v] try RequestVote(%v) to p%v\n", rf.basicInfo(), args, i)
				ok, isTimeout, toBreak := rf.sendRequestVote(i, &args, &reply, finish)
				if rf.getState() != Candidate || toBreak {
					return
				}
				if isTimeout {
					intervalFactor += 1
					Log.Printf("[%v] call RequestVote timeout to p%v, new intervalFactor %v\n",
						rf.basicInfo(), i, intervalFactor)
				} else {
					intervalFactor = 1
					if ok {
						Assert(reply.ReceiverId == i, "")
						replies <- struct {
							int
							RequestVoteReply
						}{args.Term, reply}
						Log.Printf("[%v] done RequestVote to p%v, which return %v\n",
							rf.basicInfo(), i, reply)
						return
					} else {
						Log.Printf("[%v] call RequestVote to %v fail\n", rf.basicInfo(), i)
					}
				}
				select {
				case <-finish:
					Log.Printf("[%v] finish RequestVote to p%v in advance\n", rf.basicInfo(), i)
					return
				case <-time.After(RequestVoteInterval.new() * time.Duration(intervalFactor)):
				}
			}
		}(i)

	}

	votedNum := 1
	for !rf.killed() && rf.getState() == Candidate && !rf.timeout() {
		// Only if the two terms are the same should you continue processing the reply.
		select {
		case reply := <-replies:
			rf.mu.Lock()
			if reply.int == rf.currentTerm {
				Log.Printf("[%v] get a vote reply %v, voteNum now is %v\n", rf.basicInfo(), reply, votedNum)
				if reply.Term > rf.currentTerm {
					Log.Printf("[%v] cvt to follower by RequestVote's Reply(%v) from p%v\n",
						rf.basicInfo(), reply.RequestVoteReply, reply.ReceiverId)
					rf.setTerm(reply.Term)
					rf.setState(Follower)
					rf.setVotedFor(-1)
					rf.persist(rf.newPersistant())
				} else if reply.VoteGranted {
					votedNum++
					if votedNum == len(rf.peers)/2+1 {
						Log.Printf("[%v] After get %v number of votes, I'm Leader!\n",
							rf.basicInfo(), votedNum)
						rf.setState(Leader)
						// reinitialized something after election
						rf.muLog.Lock()
						rf.matchIndex = make([]int, len(rf.peers))
						rf.nextIndex = make([]int, len(rf.peers))
						_, lastLogIndex := rf.getLastTermIndex()
						for i := range rf.nextIndex {
							rf.nextIndex[i] = lastLogIndex + 1
						}
						rf.muLog.Unlock()
					}
				}
			}
			rf.mu.Unlock()
		default:
		}
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		finish <- true
	}
	Log.Printf("[%v] election done\n", rf.basicInfo())
	wg.Wait()
}

func (rf *Raft) leaderInstallSnapshot(args InstallSnapshotArgs, i int) {
	intervalFactor := 1
	for rf.getState() == Leader && !rf.killed() {
		reply := InstallSnapshotReply{}
		Log.Printf("[%v] try InstallSnapshot(%v) to p%v\n", rf.basicInfo(), args, i)
		ok, isTimeout := rf.sendInstallSnapshot(i, &args, &reply)
		if rf.getState() != Leader {
			return
		}
		if isTimeout {
			intervalFactor += 2
			Log.Printf("[%v] call InstallSnapshot timeout to p%v\n", rf.basicInfo(), i)
		} else {
			intervalFactor = 1
			if ok {
				Log.Printf("[%v] done InstallSnapshot() to p%v, which return %v\n",
					rf.basicInfo(), i, reply)
			} else {
				Log.Printf("[%v] call InstallSnapshot to %v fail\n", rf.basicInfo(), i)
			}
			if reply.Term > rf.getTerm() {
				Log.Printf("[%v] cvt to follower by InstallSnapshot's Reply(%v) from p%v\n",
					rf.basicInfo(), reply, i)
				rf.setTerm(reply.Term)
				rf.setState(Follower)
				rf.setVotedFor(-1)
				rf.persist(rf.newPersistant())
				return
			}
		}
		time.Sleep(AppendEntryInterval.new() * time.Duration(intervalFactor))
	}
}

func (rf *Raft) doLeader() {
	msgs := make(chan AppendEntriesWrapper, len(rf.peers)-1)
	var wg sync.WaitGroup
	finish := make(chan bool, len(rf.peers)-1)
	for i := 0; rf.getState() == Leader && !rf.killed() && i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			intervalFactor := 1
			for rf.getState() == Leader && !rf.killed() {
				args := rf.NewAppendEntriesArgs(i)
				reply := AppendEntriesReply{}
				Log.Printf("[%v] try AppendEntries(%v) to p%v\n", rf.basicInfo(), args, i)
				ok, isTimeout, toBreak := rf.sendAppendEntries(i, &args, &reply, finish)
				if rf.getState() != Leader || toBreak {
					return
				}
				if isTimeout {
					intervalFactor += 2
					Log.Printf("[%v] call AppendEntries timeout to p%v\n", rf.basicInfo(), i)
				} else {
					intervalFactor = 1
					if ok {
						Assert(reply.ReceiverId == i, "")
						msgs <- AppendEntriesWrapper{args, reply}
						Log.Printf("[%v] done AppendEntries() to p%v, which return %v\n",
							rf.basicInfo(), i, reply)
					} else {
						Log.Printf("[%v] call AppendEntries to %v fail\n", rf.basicInfo(), i)
					}
				}
				select {
				case <-finish:
					return
				case <-time.After(AppendEntryInterval.new() * time.Duration(intervalFactor)):
				}
			}
		}(i)
	}

	for rf.getState() == Leader && !rf.killed() {
		select {
		case msg := <-msgs:
			if msg.arg.Term != rf.getTerm() {
				continue
			}
			rf.mu.Lock()
			if msg.reply.Term > rf.getTerm() {
				Log.Printf("[%v] cvt to follower by AppendEntried's Reply(%v) from p%v\n",
					rf.basicInfo(), msg.reply, msg.reply.ReceiverId)
				rf.setTerm(msg.reply.Term)
				rf.setState(Follower)
				rf.setVotedFor(-1)
				rf.persist(rf.newPersistant())
				rf.mu.Unlock()
				break
			}

			rf.muLog.Lock()
			if msg.reply.Success {
				rf.nextIndex[msg.reply.ReceiverId] += len(msg.arg.Entries)
				rf.matchIndex[msg.reply.ReceiverId] = rf.nextIndex[msg.reply.ReceiverId] - 1
			} else {
				if msg.reply.FailInfo.Valid {
					nextIndex := rf.nextIndex[msg.reply.ReceiverId]
					nextLogIndex := nextIndex - rf.snapshotLastIndex
					if msg.reply.FailInfo.TermIsLarge {
						initialTerm := rf.log[nextLogIndex-2].Term
						i := nextLogIndex - 2
						for ; i >= 1; i-- {
							if rf.log[i-1].Term != initialTerm {
								nextIndex = i + rf.snapshotLastIndex
								break
							}
						}
						if i == 0 {
							nextIndex = rf.snapshotLastIndex
						}
					} else if nextIndex > msg.reply.FailInfo.SuggestNextIndex {
						nextIndex = msg.reply.FailInfo.SuggestNextIndex
					} else {
						Assert(false, "")
					}
					rf.nextIndex[msg.reply.ReceiverId] = nextIndex
				} else {
					if rf.nextIndex[msg.reply.ReceiverId] > 1 {
						rf.nextIndex[msg.reply.ReceiverId]--
					}
				}
				Assert(rf.nextIndex[msg.reply.ReceiverId] > 0, "")
				if rf.nextIndex[msg.reply.ReceiverId] == rf.snapshotLastIndex {
					// should install snapshot
					wg.Add(1)
					args := InstallSnapshotArgs{
						Term:              rf.getTerm(),
						LeaderId:          rf.me,
						LastIncludedIndex: rf.snapshotLastIndex,
						LastIncludedTerm:  rf.snapshotLastTerm,
						Offset:            0,
						Data:              rf.snapshot,
						Done:              true,
					}
					go func(args InstallSnapshotArgs, i int) {
						defer wg.Done()
						rf.leaderInstallSnapshot(args, i)
					}(args, msg.reply.ReceiverId)
				}
			}
			// update commitIndex and apply cmd to state machine
			for i := len(rf.log) + rf.snapshotLastIndex; i > rf.commitIndex; i-- {
				if rf.log[i-rf.snapshotLastIndex-1].Term == rf.getTerm() {
					nrBigger := 1
					for j := 0; j < len(rf.matchIndex); j++ {
						if j == rf.me {
							continue
						}
						if rf.matchIndex[j] >= i {
							nrBigger++
						}
					}
					if nrBigger == len(rf.peers)/2+1 {
						Log.Printf("[%v] update commitIndex to %v", rf.basicInfo(), i)
						rf.commitIndex = i
						if rf.commitIndex > rf.lastApplied {
							for j := rf.lastApplied + 1; j <= rf.commitIndex; j++ {
								logIndex := j - rf.snapshotLastIndex
								rf.applyMsg(ApplyMsg{
									true, rf.log[logIndex-1].Cmd, j,
									false, nil, 0, 0})
							}
							rf.lastApplied = rf.commitIndex
						}
						break
					}
				}
			}
			rf.muLog.Unlock()
			rf.mu.Unlock()
		default:
		}
	}
	for i := 0; i < len(rf.peers)-1; i++ {
		finish <- true
	}
	Log.Printf("[%v] leader over\n", rf.basicInfo())
	wg.Wait()
}

func (rf *Raft) basicInfo() string {
	return fmt.Sprintf("p%v-t%v-%v", rf.me, rf.getTerm(), rf.getState())
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Log.Printf("[%v] ticker loop start\n", rf.basicInfo())
		// Your code here (3A)
		// Check if a leader election should be started.
		if rf.timeout() {
			if rf.getState() == Follower {
				Log.Printf("[%v] follower election timeout\n", rf.basicInfo())
				rf.setState(Candidate)
			} else if rf.getState() == Candidate {
				Log.Printf("[%v] candidate election timeout\n", rf.basicInfo())
			} else {
				Log.Panicf("Unexpect\n")
			}
			rf.election()
			if rf.getState() == Leader {
				rf.doLeader()
			}
		}

		time.Sleep(TickerInterval.new())
	}
}

// Make the service or tester wants to create a Raft server. the ports of all the Raft
// servers (including this one) are in peers[]. this server's port is peers[me].
// all the servers' peers[] arrays have the same order. persister is a place for
// this server to save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the tester or
// service expects Raft to send ApplyMsg messages. Make() must return quickly, so
// it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.setTimeoutVal()
	rf.setState(Follower)
	rf.setTerm(0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.snapshotLastIndex = 0
	rf.snapshotLastTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	Log.Printf("Make raft peer %v done\n", rf.me)
	return rf
}
