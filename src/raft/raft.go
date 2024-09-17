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
// rf.Snapshot() (index int, snapshot []byte)
//   give raft a snapshot as a mark of logs before(include) index
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
	Fixed       int
	Variability int
}

var (
	AppendEntryInterval        = Timeout{100, 50}
	RequestVoteInterval        = Timeout{100, 50}
	LeaderWaitReplyInterval    = Timeout{50, 0}
	CandidateWaitReplyInterval = Timeout{50, 0}
	ApplyUpdateInterval        = Timeout{200, 0}
	TickerInterval             = Timeout{150, 150}
	PeerTimeoutInterval        = Timeout{450, 200}
	CommandBufferSize          = 1024
	CommandNoticeInterval      = Timeout{4, 0}
	CommandNoticeTriggerNumber = 16
	ApplierBufferSize          = 1024
)

func (to *Timeout) New() time.Duration {
	if to.Variability == 0 {
		return time.Duration(to.Fixed) * time.Millisecond
	} else {
		return time.Duration(to.Fixed+(rand.Int()%to.Variability)) * time.Millisecond
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
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

func (s State) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[s]
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	rpccnt    atomic.Int64

	/* leader election */
	currentTerm  int
	votedFor     int
	currentState State
	lastTime     time.Time // last time out
	timeoutVal   time.Duration

	/* candidate temp field */
	votedNum int
	voted    []bool

	/* log */
	log []logEntry
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
	currentStateSize  atomic.Int64

	// global channel for waking up rpc senders
	wakeups []chan bool

	// command trigger buffer, used for lazy rpc wakeup
	commandTriggerBuffer chan bool
	applyBuffer          chan ApplyMsg
	applyUpdateTrigger   chan bool

	rpcIntervalController []atomic.Int32
}

type RpcInfo struct {
	AppendEntriesMaxRpcId int64
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
	return Persistence{
		rf.currentTerm,
		rf.votedFor,
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
	bytes := ToByte(persistence.state())
	rf.currentStateSize.Store(int64(len(bytes)))
	rf.persister.Save(
		bytes,
		persistence.Snapshot,
	)
}

// restore previously persisted state.
// need mu Lock
func (rf *Raft) readPersist(rstate []byte, rsnapshot []byte) {
	if rstate == nil || len(rstate) < 1 { // bootstrap without any state?
		return
	}
	var state PersistentState
	FromByte(rstate, &state)
	rf.currentStateSize.Store(int64(len(rstate)))

	rf.mu.Lock()
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
	rf.snapshot = rsnapshot
	rf.snapshotLastIndex = state.SnapshotLastIndex
	rf.snapshotLastTerm = state.SnapshotLastTerm
	rf.mu.Unlock()
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logIndex := index - rf.snapshotLastIndex
	if logIndex <= 0 {
		return
	}

	Log.Printf("[%v] snapshot index %v\n", rf.basicInfo(), index)
	rf.snapshot = snapshot
	rf.snapshotLastIndex = index
	rf.snapshotLastTerm = rf.log[logIndex-1].Term
	if logIndex == len(rf.log) {
		rf.log = []logEntry{}
	} else if logIndex > len(rf.log) {
		Panic()
	} else {
		// index-1+1 = index
		rf.log = append(rf.log[logIndex:])
	}
	rf.persist(rf.newPersistant())
}

type logEntry struct {
	Cmd  interface{}
	Term int
}

func (rf *Raft) getStateRLock() State {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentState
}

func (rf *Raft) getTermRLock() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) getVotedForRLock() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.votedFor
}

func (rf *Raft) timeout() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return time.Now().Sub(rf.lastTime) > rf.timeoutVal
}

func (rf *Raft) setTimeoutVal() {
	rf.lastTime = time.Now()
	rf.timeoutVal = PeerTimeoutInterval.New()
}

// need mu.RLock()
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
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	Log.Printf("GetState[%v](T: %v, State: %v)\n",
		rf.me, rf.currentTerm, rf.currentState)
	return rf.currentTerm, rf.currentState == Leader
}

// return ok
func (rf *Raft) sendRpc(
	logPrefix string, peer int, meth string, args interface{}, reply interface{}) bool {
	methname := meth[5:]
	Log.Printf("%v try %v(%v) to p%v\n", logPrefix, methname, args, peer)
	start := time.Now()
	ok := rf.peers[peer].Call(meth, args, reply)
	usedMilli := time.Now().Sub(start).Microseconds() / 1000.0
	if ok {
		Log.Printf("%v done %v(use %v ms) to p%v, which return %v\n",
			logPrefix, methname, usedMilli, peer, reply)
		rf.rpcIntervalController[peer].Store(0)
	} else {
		Log.Printf("%v call %v(use %v ms) to p%v failed\n",
			logPrefix, methname, usedMilli, peer)
		rf.rpcIntervalController[peer].Add(1)
	}
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	RpcId             int64
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("{Term: %v, SnapIndex: %v, SnapTerm: %v}",
		args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term       int
	ReceiverId int
	Info       string
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("{Term: %v, ReceiverId: %v, Info: \"%v\"}", reply.Term, reply.ReceiverId, reply.Info)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	AssertNoReason(args.LastIncludedIndex >= 1)
	AssertNoReason(args.LastIncludedTerm >= 1)
	AssertNoReason(args.LastIncludedTerm >= rf.snapshotLastTerm)

	logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), args.RpcId)
	reply.Term = rf.currentTerm
	reply.ReceiverId = rf.me
	if args.Term < reply.Term {
		reply.Info = "Leader's term is smaller than me"
		return
	}
	if args.LastIncludedIndex <= rf.snapshotLastIndex {
		reply.Info = "Old snapshot rpc"
		return
	}

	mayLogIndex := args.LastIncludedIndex - rf.snapshotLastIndex
	if mayLogIndex > len(rf.log) || len(rf.log) == 0 || rf.log[mayLogIndex-1].Term != rf.snapshotLastTerm {
		reply.Info = "Clear all log"
		rf.log = []logEntry{}
	} else {
		reply.Info = "Retain some log"
		rf.log = append(rf.log[mayLogIndex:])
	}

	Log.Printf("%v InstallSnapshot ssLastIndex %v, commitIndex %v, lastApplied %v\n",
		logPrefix, rf.snapshotLastIndex, rf.commitIndex, rf.lastApplied)
	rf.snapshotLastIndex = args.LastIncludedIndex
	rf.snapshotLastTerm = args.LastIncludedTerm
	if rf.snapshotLastIndex > rf.commitIndex {
		rf.commitIndex = rf.snapshotLastIndex
		rf.triggerApplyUpdate()
	}
	rf.snapshot = args.Data
	rf.persist(rf.newPersistant())
}

func (rf *Raft) triggerApplyUpdate() {
	select {
	case rf.applyUpdateTrigger <- true:
	default:
	}
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
	RpcId       int64
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

type RequestVoteWrapper struct {
	args  RequestVoteArgs
	reply RequestVoteReply
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("{T: %v, Id: %v, LastLogIndex: %v, LastLogTerm: %v}",
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

func (args RequestVoteReply) String() string {
	return fmt.Sprintf("{T: %v, Voted: %v}", args.Term, args.VoteGranted)
}

func (rf *Raft) newRpcId() int64 {
	return (rf.rpccnt.Add(1) << 8) | int64(rf.me)
}

// return rpccnt and server id
func (rf *Raft) fromRpcId(rpcId int64) (int64, int) {
	return rpcId >> 8, int(rpcId & 0xff)
}

func (rf *Raft) newRequestVoteArgs(_ int) RequestVoteArgs {
	lastLogTerm, lastLogIndex := rf.getLastTermIndex()
	args := RequestVoteArgs{rf.currentTerm, rf.me,
		lastLogIndex, lastLogTerm, rf.newRpcId()}
	return args
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), args.RpcId)

	reply.Term = rf.currentTerm
	reply.ReceiverId = rf.me
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		Log.Printf("%v cvt to follower by RequestVoteCall(%v)\n",
			logPrefix, args)
		rf.onBiggerTerm(args.Term)
		rf.persist(rf.newPersistant())
	}

	// INFO page6, "first-come-first-served" basis
	votedFor := rf.votedFor
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
		lastLogTerm, lastLogIndex := rf.getLastTermIndex()
		if lastLogIndex == 0 || lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			Log.Printf("%v give p%v vote(newly-give: %v)\n",
				logPrefix, args.CandidateId, votedFor != args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.setTimeoutVal()
		}
	}
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
	RpcId        int64
}

type AppendEntriesReplyFailInfo struct {
	Valid         bool
	ConflictTerm  int
	ConflictIndex int
}

type AppendEntriesReply struct {
	/* currentTerm, for leader to update itself */
	Term int
	/* true if follower contained entry matching prevLogIndex and prevLogTerm */
	Success          bool
	SuccessNextIndex int
	FailInfo         AppendEntriesReplyFailInfo
	// for debug
	ReceiverId int
	Info       string
}

type AppendEntriesWrapper struct {
	arg   AppendEntriesArgs
	reply AppendEntriesReply
}

type InstallSnapshotWrapper struct {
	arg   InstallSnapshotArgs
	reply InstallSnapshotReply
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term: %v, Id: %v, PrevLogIndex: %v, PrevLogTerm: %v, "+
		"EntriesLen: %v, LeaderCommit: %v}",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
		len(args.Entries), args.LeaderCommit)
}

func (reply AppendEntriesReply) String() string {
	var ret string
	if reply.FailInfo.Valid {
		ret = fmt.Sprintf("{Term: %v, Suc: %v, Info: \"%v\", FailInfo: %v}",
			reply.Term, reply.Success, reply.Info, reply.FailInfo)
	} else {
		ret = fmt.Sprintf("{Term: %v, Suc: %v, Info: \"%v\"}",
			reply.Term, reply.Success, reply.Info)
	}
	return ret
}

// NewAppendEntriesArgs need rf.mu.RLock()
func (rf *Raft) NewAppendEntriesArgs(i int) AppendEntriesArgs {
	var prevTerm int
	var prevLogIndex int
	var entries []logEntry
	nextLogIndex := rf.nextIndex[i] - rf.snapshotLastIndex
	Assert(nextLogIndex > 0, "nextLogIndex invalid while newAppendEntriesArgs!")

	if nextLogIndex <= len(rf.log) {
		entries = append(entries, rf.log[nextLogIndex-1:]...)
	}
	prevTerm = 0
	prevLogIndex = nextLogIndex - 1
	if prevLogIndex > 0 && prevLogIndex <= len(rf.log) {
		prevTerm = rf.log[prevLogIndex-1].Term
	} else {
		prevTerm = rf.snapshotLastTerm
	}
	args := AppendEntriesArgs{rf.currentTerm, rf.me,
		rf.nextIndex[i] - 1, prevTerm,
		entries, rf.commitIndex, rf.newRpcId()}
	return args
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), args.RpcId)

	reply.Term = rf.currentTerm
	reply.ReceiverId = rf.me

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Info = "Leader's term is lower than me"
		return
	} else if args.Term > rf.currentTerm {
		Log.Printf("%v cvt to follower by AppendEntriesCall(%v)\n",
			logPrefix, args)
		rf.onBiggerTerm(args.Term)
		logPrefix = fmt.Sprintf("[%v][%v]", rf.basicInfo(), args.RpcId)
		rf.persist(rf.newPersistant())
	} else if rf.currentState == Candidate {
		rf.currentState = Follower
		rf.votedFor = -1
	}
	rf.setTimeoutVal()

	_, lastLogIndex := rf.getLastTermIndex()
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.Info = "Leader's PreLogIndex is bigger than my lastLogIndex"
		reply.FailInfo.Valid = true
		reply.FailInfo.ConflictIndex = lastLogIndex + 1
		return
	}
	// snapshot is not consistent with leader, maybe doing installSnapshot
	if args.PrevLogIndex < rf.snapshotLastIndex {
		reply.Success = false
		reply.Info = "Leader's PreLogIndex is smaller than my snapshotLastIndex"
		return
	}

	prevTerm := 0
	prevIndexInLog := args.PrevLogIndex - rf.snapshotLastIndex
	if prevIndexInLog == 0 {
		prevTerm = rf.snapshotLastTerm
	} else {
		prevTerm = rf.log[prevIndexInLog-1].Term
	}

	lastLen := len(rf.log)
	if args.PrevLogIndex >= 1 && prevTerm != args.PrevLogTerm {
		// snapshot is not consistent with leader
		reply.Success = false
		reply.FailInfo.Valid = true

		reply.FailInfo.ConflictIndex = args.PrevLogIndex
		for i := prevIndexInLog - 1; i >= 1; i-- {
			if rf.log[i-1].Term != prevTerm {
				reply.FailInfo.ConflictIndex = i + 1
				break
			}
		}
		reply.FailInfo.ConflictTerm = prevTerm
		/*
			If an existing entry conflicts with a new one (same index
			but different terms), delete the existing entry and all that
			follow it (§5.3).
		*/
		rf.log = append(rf.log[:prevIndexInLog-1])
		Log.Printf("%v truncate %v logs, now %v\n", logPrefix, lastLen-len(rf.log), len(rf.log))
		rf.persist(rf.newPersistant())
		return
	}

	// Append any new entries not already in the log.
	if len(args.Entries) != 0 {
		appended := 0
		overrided := 0
		revised := 0
		for i := 0; i < len(args.Entries); i++ {
			toIndex := i + prevIndexInLog + 1
			if len(rf.log) < toIndex {
				rf.log = append(rf.log, args.Entries[i])
				appended++
			} else {
				if rf.log[toIndex-1].Term != args.Entries[i].Term {
					revised++
					rf.log = append(rf.log[:toIndex])
					rf.log[toIndex-1] = args.Entries[i]
				} else {
					overrided++
				}
			}
		}
		Log.Printf("%v sync logs(append %v, revise %v, override %v), now %v\n",
			logPrefix, appended, revised, overrided, len(rf.log))
		rf.persist(rf.newPersistant())
	}
	reply.SuccessNextIndex = rf.snapshotLastIndex + len(rf.log) + 1

	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := Min(len(rf.log)+rf.snapshotLastIndex, args.LeaderCommit)
		Log.Printf("%v commitIndex from %v to %v\n", logPrefix, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.triggerApplyUpdate()
	}
	reply.Success = true
}

func (rf *Raft) applyMsg(msg *ApplyMsg) {
	var appliedMsgs []ApplyMsg
	appliedMsgs = append(appliedMsgs, *msg)
	rf.applyMsgs(appliedMsgs)
}

// must hold mu, since basicInfo need lock
func (rf *Raft) applyMsgs(msgs []ApplyMsg) {
	for _, msg := range msgs {
		rf.applyBuffer <- msg

		if msg.CommandValid {
			Log.Printf("[p%v] applied CMD {index: %v, cmd: %v} to buffer\n",
				rf.me, msg.CommandIndex, cmd2str(msg.Command))
		} else if msg.SnapshotValid {
			showLen := 10
			if len(msg.Snapshot) < showLen {
				showLen = len(msg.Snapshot)
			}
			Log.Printf("[p%v] applied SNAPSHOT {index: %v, term: %v, data: %v} to buffer\n",
				rf.me, msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot[:showLen])
		} else {
			Panic()
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.currentState == Leader
	if rf.killed() || !isLeader {
		return 0, 0, false
	}

	term := rf.currentTerm
	index := rf.snapshotLastIndex + len(rf.log) + 1
	rf.log = append(rf.log, logEntry{command, rf.currentTerm})
	if len(rf.commandTriggerBuffer) < CommandBufferSize {
		rf.commandTriggerBuffer <- true
	}
	rf.persist(rf.newPersistant())
	Log.Printf("[%v] Start(%v) -> (index:%v,term:%v,isLeader:%v)",
		rf.basicInfo(), cmd2str(command), index, rf.currentTerm, isLeader)
	return index, term, isLeader
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

func (rf *Raft) intervalFactor(peer int) time.Duration {
	ctl := rf.rpcIntervalController[peer].Load()
	if ctl == 0 {
		return 1
	} else {
		val := 1.0
		for ctl > 0 {
			ctl--
			val *= 2
			if val >= 8 {
				break
			}
		}
		return time.Duration(val)
	}
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.setTimeoutVal()
	rf.votedFor = rf.me
	rf.votedNum = 1
	rf.voted = make([]bool, len(rf.peers))
	rf.persist(rf.newPersistant())
	rf.mu.Unlock()

	var wg sync.WaitGroup
	finish := make(chan bool)

	// While waiting for votes, a candidate may receive an
	// AppendEntries RPC from another server claiming to be
	// leader. which may let the candidate become follower state.
	msgs := make(chan RequestVoteWrapper, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for !rf.killed() && rf.getStateRLock() == Candidate && !rf.timeout() {
				go func() {
					rf.mu.RLock()
					args := rf.newRequestVoteArgs(i)
					reply := RequestVoteReply{}
					logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), args.RpcId)
					rf.mu.RUnlock()
					ok := rf.sendRpc(logPrefix, i, "Raft.RequestVote", &args, &reply)
					if ok {
						Assert(reply.ReceiverId == i, "")
						if !rf.killed() && rf.getStateRLock() == Candidate &&
							!rf.timeout() && args.Term == rf.getTermRLock() {
							msgs <- RequestVoteWrapper{args, reply}
						}
					}
				}()
				select {
				case <-finish:
					return
				case <-time.After(RequestVoteInterval.New() * rf.intervalFactor(i)):
				}
			}
		}(i)
	}

	for !rf.killed() && rf.getStateRLock() == Candidate && !rf.timeout() {
		// Only if the two terms are the same should you continue processing the reply.
		select {
		case msg := <-msgs:
			if rf.getStateRLock() != Candidate || rf.killed() || rf.timeout() {
				break
			}
			if msg.args.Term != rf.getTermRLock() {
				break
			}
			rf.onRequestVoteReply(&msg)
		case <-time.After(CandidateWaitReplyInterval.New()):
		}
	}

	close(finish)
	Log.Printf("[%v] election done\n", rf.basicInfoRLock())
	wg.Wait()
}

func (rf *Raft) onRequestVoteReply(msg *RequestVoteWrapper) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), msg.args.RpcId)

	if msg.reply.Term > rf.currentTerm {
		Log.Printf("%v cvt to follower by RequestVote's Reply(%v) from p%v\n",
			logPrefix, msg.reply, msg.reply.ReceiverId)
		rf.onBiggerTerm(msg.reply.Term)
		rf.persist(rf.newPersistant())
	} else if msg.reply.VoteGranted {
		// avoid a peer vote multiple time
		if msg.reply.VoteGranted && !rf.voted[msg.reply.ReceiverId] {
			rf.voted[msg.reply.ReceiverId] = true
			rf.votedNum = rf.votedNum + 1
			Log.Printf("%v get a vote from %v, voteNum now is %v\n",
				logPrefix, msg.reply.ReceiverId, rf.votedNum)
		}
		if rf.votedNum == len(rf.peers)/2+1 {
			Log.Printf("%v After get %v number of votes, I'm Leader!\n",
				logPrefix, rf.votedNum)
			rf.currentState = Leader
			// reinitialized something after election
			rf.matchIndex = make([]int, len(rf.peers))
			rf.nextIndex = make([]int, len(rf.peers))
			_, lastLogIndex := rf.getLastTermIndex()
			for i := range rf.nextIndex {
				rf.nextIndex[i] = lastLogIndex + 1
			}
		}
	}
}

// need mu.Lock()
// return isUpdated
func (rf *Raft) leaderUpdateCommit(rpcId int64) {
	// update commitIndex and apply cmd to state machine
	for i := len(rf.log) + rf.snapshotLastIndex; i > Max(rf.commitIndex, rf.snapshotLastIndex); i-- {
		if len(rf.log) != 0 && rf.log[i-rf.snapshotLastIndex-1].Term == rf.currentTerm {
			nrBigger := 1
			for j := 0; j < len(rf.peers); j++ {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= i {
					nrBigger++
				}
			}
			if nrBigger >= len(rf.peers)/2+1 {
				Log.Printf("[%v][%v] update commitIndex to %v", rf.basicInfo(), rpcId, i)
				rf.commitIndex = i
				rf.triggerApplyUpdate()
				break
			}
		}
	}
}

func (rf *Raft) onBiggerTerm(term int) {
	rf.currentTerm = term
	rf.currentState = Follower
	rf.votedFor = -1
}

// wg is used to help caller know if this function it's done
func (rf *Raft) leaderOnAppendEntriesReply(msg *AppendEntriesWrapper) {
	rf.mu.Lock()

	logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), msg.arg.RpcId)

	if msg.reply.Term > rf.currentTerm {
		Log.Printf("%v cvt to follower by AppendEntried's Reply(%v) from p%v\n",
			logPrefix, msg.reply, msg.reply.ReceiverId)
		rf.onBiggerTerm(msg.reply.Term)
		rf.persist(rf.newPersistant())
		rf.mu.Unlock()
		return
	}

	needWakeup := false
	initialNextIndex := rf.nextIndex[msg.reply.ReceiverId]
	if msg.reply.Success {
		// use SuccessNextIndex to avoid reupdate nextindex
		if msg.reply.SuccessNextIndex > rf.nextIndex[msg.reply.ReceiverId] {
			selfMaxNextIndex := len(rf.log) + rf.snapshotLastIndex + 1
			rf.nextIndex[msg.reply.ReceiverId] = Min(msg.reply.SuccessNextIndex, selfMaxNextIndex)
			rf.matchIndex[msg.reply.ReceiverId] = rf.nextIndex[msg.reply.ReceiverId] - 1
		}
	} else {
		if !msg.reply.FailInfo.Valid {
			if rf.nextIndex[msg.reply.ReceiverId] > 1 {
				rf.nextIndex[msg.reply.ReceiverId]--
			}
		} else {
			nextIndex := rf.nextIndex[msg.reply.ReceiverId]
			conflictIndex := msg.reply.FailInfo.ConflictIndex
			conflictTerm := msg.reply.FailInfo.ConflictTerm
			for i := nextIndex - rf.snapshotLastIndex - 1; i >= 1 && i <= len(rf.log); i-- {
				if rf.log[i-1].Term == conflictTerm {
					nextIndex = i + 1
					break
				}
			}
			if nextIndex == rf.nextIndex[msg.reply.ReceiverId] {
				nextIndex = conflictIndex
			}
			rf.nextIndex[msg.reply.ReceiverId] = nextIndex
		}
		needWakeup = true
	}
	Log.Printf("%v On AppendEntriesReply nextIndex[%v] from %v to %v\n",
		logPrefix, msg.reply.ReceiverId, initialNextIndex, rf.nextIndex[msg.reply.ReceiverId])
	Assert(rf.nextIndex[msg.reply.ReceiverId] > 0, "nextIndex < 1!")
	rf.leaderUpdateCommit(msg.arg.RpcId)
	rf.mu.Unlock()
	if needWakeup {
		Log.Printf("%v wakeup p%v\n", logPrefix, msg.reply.ReceiverId)
		rf.wakeupLeaderRpc(msg.reply.ReceiverId)
	}
}

func (rf *Raft) leaderOnInstallSnapshotReply(msg *InstallSnapshotWrapper) {
	args := msg.arg
	reply := msg.reply
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), args.RpcId)
	if reply.Term > rf.currentTerm {
		Log.Printf("%v cvt to follower by InstallSnapshot's Reply(%v) from p%v\n",
			logPrefix, reply, reply.ReceiverId)
		rf.onBiggerTerm(reply.Term)
		rf.persist(rf.newPersistant())
		return
	}
	i := msg.reply.ReceiverId
	initialNextIndex := rf.nextIndex[i]
	if rf.nextIndex[i] <= args.LastIncludedIndex {
		rf.nextIndex[i] = args.LastIncludedIndex + 1
		rf.matchIndex[i] = rf.nextIndex[i] - 1
		rf.leaderUpdateCommit(msg.arg.RpcId)
		if rf.nextIndex[i] <= rf.snapshotLastIndex+len(rf.log) {
			Log.Printf("%v wakeup p%v\n", logPrefix, msg.reply.ReceiverId)
			rf.mu.Unlock()
			rf.wakeupLeaderRpc(msg.reply.ReceiverId)
			rf.mu.Lock()
		}
	}
	Log.Printf("%v On InstallSnapsthoReply nextIndex[%v] from %v to %v\n",
		logPrefix, msg.reply.ReceiverId, initialNextIndex, rf.nextIndex[msg.reply.ReceiverId])
}

func (rf *Raft) wakeupLeaderRpc(peer int) {
	select {
	case rf.wakeups[peer] <- true:
	default:
	}
}

func (rf *Raft) wakeupLeaderAllRpc() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.wakeupLeaderRpc(i)
	}
}

func (rf *Raft) doLeader() {
	msgs := make(chan AppendEntriesWrapper, len(rf.peers)-1)
	ssmsgs := make(chan InstallSnapshotWrapper, len(rf.peers)-1)
	finish := make(chan bool)
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for rf.getStateRLock() == Leader && !rf.killed() {
				go func() {
					rf.mu.RLock()
					if rf.currentState != Leader || rf.killed() {
						rf.mu.RUnlock()
						return
					}
					if rf.nextIndex[i] > rf.snapshotLastIndex {
						args := rf.NewAppendEntriesArgs(i)
						logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), args.RpcId)
						rf.mu.RUnlock()
						reply := AppendEntriesReply{}
						ok := rf.sendRpc(logPrefix, i, "Raft.AppendEntries", &args, &reply)
						if ok {
							Assert(reply.ReceiverId == i, "")
							if rf.getStateRLock() == Leader && !rf.killed() && rf.getTermRLock() == args.Term {
								msgs <- AppendEntriesWrapper{args, reply}
							}
						}
					} else {
						args := InstallSnapshotArgs{
							Term:              rf.currentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.snapshotLastIndex,
							LastIncludedTerm:  rf.snapshotLastTerm,
							Data:              rf.snapshot,
							RpcId:             rf.newRpcId(),
						}
						logPrefix := fmt.Sprintf("[%v][%v]", rf.basicInfo(), args.RpcId)
						rf.mu.RUnlock()
						reply := InstallSnapshotReply{}
						ok := rf.sendRpc(logPrefix, i, "Raft.InstallSnapshot", &args, &reply)
						if ok {
							Assert(reply.ReceiverId == i, "")
							if rf.getStateRLock() == Leader && !rf.killed() && rf.getTermRLock() == args.Term {
								ssmsgs <- InstallSnapshotWrapper{args, reply}
							}
						}
					}
				}()
				select {
				case <-finish:
					return
				case <-rf.wakeups[i]:
				case <-time.After(AppendEntryInterval.New() * rf.intervalFactor(i)):
				}
			}
		}(i)
	}

	for rf.getStateRLock() == Leader && !rf.killed() {
		select {
		case msg := <-msgs:
			if rf.getStateRLock() != Leader || rf.killed() {
				break
			}
			if msg.arg.Term != rf.getTermRLock() {
				break
			}
			rf.leaderOnAppendEntriesReply(&msg)
		case ssmsg := <-ssmsgs:
			if rf.getStateRLock() != Leader || rf.killed() {
				break
			}
			if ssmsg.arg.Term != rf.getTermRLock() {
				break
			}
			rf.leaderOnInstallSnapshotReply(&ssmsg)
		case <-time.After(LeaderWaitReplyInterval.New()):
		}
	}
	close(finish)
	Log.Printf("[%v] leader over\n", rf.basicInfoRLock())
	wg.Wait()
}

func (rf *Raft) basicInfo() string {
	return fmt.Sprintf("t%v-%v@p%v", rf.currentTerm, rf.currentState, rf.me)
}

func (rf *Raft) basicInfoRLock() string {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return fmt.Sprintf("t%v-%v@p%v", rf.currentTerm, rf.currentState, rf.me)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		if rf.timeout() {
			rf.mu.Lock()
			if rf.currentState == Follower {
				Log.Printf("[%v] follower election timeout\n", rf.basicInfo())
				rf.currentState = Candidate
			} else if rf.currentState == Candidate {
				Log.Printf("[%v] candidate election timeout\n", rf.basicInfo())
			} else {
				Log.Panicf("Unexpect\n")
			}
			rf.mu.Unlock()
			for rf.timeout() {
				rf.election()
			}
			if rf.getStateRLock() == Leader {
				rf.doLeader()
			}
		}

		time.Sleep(TickerInterval.New())
	}
}

func (rf *Raft) commandNoticer() {
	for {
		cnt := 0
		<-rf.commandTriggerBuffer
		cnt++
		working := true
		startTime := time.Now()
		for working {
			select {
			case <-rf.commandTriggerBuffer:
				cnt++
				if cnt == CommandNoticeTriggerNumber {
					working = false
					break
				}
				if time.Now().Sub(startTime) > CommandNoticeInterval.New() {
					working = false
					break
				}
			case <-time.After(CommandNoticeInterval.New()):
				working = false
				break
			}
		}
		Log.Printf("wakeup by %v cmd\n", cnt)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.wakeupLeaderRpc(i)
		}
	}
}

func (rf *Raft) messageApplier() {
	for msg := range rf.applyBuffer {
		rf.applyCh <- msg
	}
}

func (rf *Raft) applyRoutine() {
	for {
		select {
		case <-rf.applyUpdateTrigger:
		case <-time.After(ApplyUpdateInterval.New()):
		}
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			var appliedMsgs []ApplyMsg
			for j := rf.lastApplied + 1; j <= rf.commitIndex; j++ {
				logIndex := j - rf.snapshotLastIndex
				if logIndex <= 0 {
					rf.applyMsg(&ApplyMsg{
						false, nil, 0,
						true, rf.snapshot, rf.snapshotLastTerm, rf.snapshotLastIndex,
					})
					j = rf.snapshotLastIndex
					continue
				}
				cmd := rf.log[logIndex-1].Cmd
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					true, cmd, j,
					false, nil, 0, 0})
			}
			rf.lastApplied = rf.commitIndex
			isLeader := rf.currentState == Leader
			rf.mu.Unlock()
			rf.applyMsgs(appliedMsgs)
			if isLeader {
				rf.wakeupLeaderAllRpc()
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) CurrentStateSize() int64 {
	return rf.currentStateSize.Load()
}

func (rf *Raft) SnapshotLastIndex() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.snapshotLastIndex
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
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.snapshotLastIndex = 0
	rf.snapshotLastTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.rpcIntervalController = make([]atomic.Int32, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.wakeups = append(rf.wakeups, make(chan bool, 1))
		rf.rpcIntervalController[i] = atomic.Int32{}
		rf.rpcIntervalController[i].Store(0)
	}
	rf.applyBuffer = make(chan ApplyMsg, ApplierBufferSize)
	rf.commandTriggerBuffer = make(chan bool, CommandBufferSize)
	rf.applyUpdateTrigger = make(chan bool, 1)

	// dispatch commandNoticer for rf.Start
	go rf.commandNoticer()

	// dispatch message applier
	go rf.messageApplier()

	go rf.applyRoutine()

	// start ticker goroutine to start elections
	go rf.ticker()

	Log.Printf("Make raft peer %v done\n", rf.me)
	return rf
}
