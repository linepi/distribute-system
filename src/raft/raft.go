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

// as each Raft peer becomes aware that successive log entries are
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
  applyCh   chan ApplyMsg

  /* leader election */
  muCom sync.RWMutex
	currentTerm int
	votedFor int
	currentState State
	lastTime     time.Time // last time out
	timeoutVal   int64     // milliseconds

  /* log */
  muLog sync.RWMutex
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
}

type logEntry struct {
  Cmd interface{}
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
	return time.Now().Sub(rf.lastTime).Milliseconds() > rf.timeoutVal
}

func (rf *Raft) setTimeoutVal() {
  rf.muCom.Lock()
  defer rf.muCom.Unlock()
	rf.lastTime = time.Now()
	rf.timeoutVal = rand.Int63()%150 + 550
}

func (rf *Raft) addLog(le logEntry) {
  rf.muLog.Lock()
  defer rf.muLog.Unlock()
  rf.log = append(rf.log, le)
}

func (rf *Raft) getLog(i int) logEntry {
  rf.muLog.RLock()
  defer rf.muLog.RUnlock()
  return rf.log[i]
}

func (rf *Raft) getLastCommitLog() logEntry {
  return rf.getLog(rf.getCommitIndex())
}

func (rf *Raft) getLastAppliedLog() logEntry {
  return rf.getLog(rf.getLastApplied())
}

func (rf *Raft) getLastLog() logEntry {
  rf.muLog.RLock()
  defer rf.muLog.RUnlock()
  return rf.log[len(rf.log) - 1]
}

func (rf *Raft) getLastLogIndex() int {
  rf.muLog.RLock()
  defer rf.muLog.RUnlock()
  return len(rf.log) - 1
}

func (rf *Raft) getCommitIndex() int {
  rf.muLog.RLock()
  defer rf.muLog.RUnlock()
  return rf.commitIndex
}

func (rf *Raft) setCommitIndex(v int) {
  rf.muLog.Lock()
  defer rf.muLog.Unlock()
  rf.commitIndex = v
}

func (rf *Raft) getLastApplied() int {
  rf.muLog.RLock()
  defer rf.muLog.RUnlock()
  return rf.lastApplied
}

func (rf *Raft) setLastApplied(v int) {
  rf.muLog.Lock()
  defer rf.muLog.Unlock()
  rf.lastApplied = v
}

func (rf *Raft) getNextIndex(i int) int {
  rf.muLog.RLock()
  defer rf.muLog.RUnlock()
  return rf.nextIndex[i]
}

func (rf *Raft) addNextIndex(v int) {
  rf.muLog.Lock()
  defer rf.muLog.Unlock()
  rf.nextIndex = append(rf.nextIndex, v)
}

func (rf *Raft) setNextIndex(s []int) {
  rf.muLog.Lock()
  defer rf.muLog.Unlock()
  rf.nextIndex = s
}

func (rf *Raft) getMatchIndex(i int) int {
  rf.muLog.RLock()
  defer rf.muLog.RUnlock()
  return rf.matchIndex[i]
}

func (rf *Raft) addMatchIndex(v int) {
  rf.muLog.Lock()
  defer rf.muLog.Unlock()
  rf.matchIndex = append(rf.matchIndex, v)
}

func (rf *Raft) setMatchIndex(s []int) {
  rf.muLog.Lock()
  defer rf.muLog.Unlock()
  rf.matchIndex = s
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	Log.Printf("GetState[%v](T: %v, State: %v)\n",
		rf.me, rf.getTerm(), rf.getState())
	return rf.getTerm(), rf.getState() == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
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

// example RequestVote RPC reply structure.
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
	return fmt.Sprintf("{myT: %v, myId: %v}", args.Term, args.CandidateId)
}

func (args RequestVoteReply) String() string {
	return fmt.Sprintf("{otherT: %v, Voted: %v}", args.Term, args.VoteGranted)
}

func (rf *Raft) NewRequestVoteArgs(i int) RequestVoteArgs {
  rf.muLog.RLock()
  lastLogTerm := -1
  if len(rf.log) > 0 {
    lastLogTerm = rf.log[len(rf.log)-1].Term
  }
  args := RequestVoteArgs{rf.getTerm(), rf.me, 
            len(rf.log)-1, lastLogTerm}
  rf.muLog.RUnlock()
  return args
}

// example RequestVote RPC handler.
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
    if rf.getLastLogIndex() == -1 ||
       rf.getLastLog().Term < args.LastLogTerm || 
      (rf.getLastLog().Term == args.LastLogTerm && 
       rf.getLastLogIndex() <= args.LastLogIndex) {
      reply.VoteGranted = true
      Log.Printf("[%v] give p%v vote(newly-give: %v)\n", 
        rf.basicInfo(), args.CandidateId, votedFor != args.CandidateId)
      rf.setVotedFor(args.CandidateId)
      rf.setTimeoutVal()
    }
	}
}

// return (ok, isTimeout)
func (rf *Raft) sendRequestVote(
    peer int, args *RequestVoteArgs, reply *RequestVoteReply) (bool, bool) {
	done := make(chan bool)
	ok := false
	go func() {
		ok = rf.peers[peer].Call("Raft.RequestVote", args, reply)
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(time.Duration(300) * time.Millisecond):
		return false, true
	}
	return ok, false
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

type AppendEntriesReply struct {
	/* currentTerm, for leader to update itself */
	Term int
	/* true if follower contained entry matching prevLogIndex and prevLogTerm */
	Success bool

	// for debug
	ReceiverId int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{myT: %v, myId: %v}", args.Term, args.LeaderId)
}

func (args AppendEntriesReply) String() string {
	return fmt.Sprintf("{otherT: %v, Suc: %v}", args.Term, args.Success)
}

func (rf *Raft) NewAppendEntriesArgs(i int) AppendEntriesArgs {
  rf.muLog.RLock()
  nextIndex := rf.getNextIndex(i)
  entries := []logEntry{}
  if len(rf.log)-1 >= nextIndex {
    // for simplicity just append one entry
    entries = append(entries, rf.log[nextIndex])
  }
  prevLogTerm := -1
  prevLogIndex := -1
  if nextIndex > 0 && nextIndex - 1 < len(rf.log) {
    prevLogTerm = rf.log[nextIndex-1].Term
    prevLogIndex = nextIndex - 1
  }
  args := AppendEntriesArgs{rf.getTerm(), rf.me,
            prevLogIndex, prevLogTerm,
            entries, rf.getCommitIndex()}
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
		rf.setState(Follower)
	}
	rf.setTimeoutVal()

  if args.PrevLogIndex == -1 {
		reply.Success = false
		return
  }

  if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		return
  } else if rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		return
  }

  rf.muLog.Lock()
  if len(rf.log) == args.PrevLogIndex + 1 {
    rf.log = append(rf.log, args.Entries...)
  } else if rf.log[args.PrevLogIndex + 1].Term != args.Entries[0].Term {
    rf.log = rf.log[:args.PrevLogIndex + 1]
    rf.log = append(rf.log, args.Entries...)
  }
  rf.muLog.Unlock()

  if args.LeaderCommit > rf.getCommitIndex() {
    rf.setCommitIndex(
      min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries)))
  }
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(
	peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) (bool, bool) {
	done := make(chan bool)
	ok := false
	go func() {
		ok = rf.peers[peer].Call("Raft.AppendEntries", args, reply)
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(time.Duration(300) * time.Millisecond):
		return false, true
	}
	return ok, false
}

// the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := rf.getState() == Leader
  if !isLeader {
    return -1, -1, false
  }

	index := rf.getLastLogIndex() + 1
	term := rf.getTerm()
  rf.addLog(logEntry{command, term})
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
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

	var wg sync.WaitGroup
	var done atomic.Bool
	done.Store(false)

	// While waiting for votes, a candidate may receive an
	// AppendEntries RPC from another server claiming to be
	// leader. which may let the candidate become follower state.
	replies := make(chan struct {
		int
		RequestVoteReply
	}, len(rf.peers)-1)
	for i := 0; rf.killed() == false && rf.getState() == Candidate &&
		!rf.timeout() && i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for rf.killed() == false && rf.getState() == Candidate &&
				!rf.timeout() && !done.Load() {
        args := rf.NewRequestVoteArgs(i)
				reply := RequestVoteReply{}
				Log.Printf("[%v] try vote from p%v\n", rf.basicInfo(), i)
				ok, isTimeout := rf.sendRequestVote(i, &args, &reply)
				if rf.getState() != Candidate {
					break
				}
				if ok {
					Assert(reply.ReceiverId == i, "")
					replies <- struct {
						int
						RequestVoteReply
					}{args.Term, reply}
					Log.Printf("[%v] done RequestVote(%v, %v) to %v\n",
						rf.basicInfo(), args, reply, i)
					break
				} else if isTimeout {
					Log.Printf("[%v] call RequestVote timeout\n", rf.basicInfo())
				} else {
					Log.Printf("[%v] call RequestVote to %v fail\n", rf.basicInfo(), i)
				}
				time.Sleep(time.Duration(rand.Int63()%100+100) * time.Millisecond)
			}
		}(i)

	}

	votedNum := 1
	for rf.killed() == false && rf.getState() == Candidate && !rf.timeout() {
		// Only if the two terms are the same should you continue processing the reply.
		select {
		case reply := <-replies:
			rf.mu.Lock()
			if reply.int == rf.currentTerm {
				if reply.Term > rf.currentTerm {
					Log.Printf("[%v] cvt to follower by RequestVote's Reply(%v) from p%v\n",
						rf.basicInfo(), reply.RequestVoteReply, reply.ReceiverId)
					rf.setTerm(reply.Term)
					rf.setState(Follower)
					rf.setVotedFor(-1)
				} else if reply.VoteGranted {
					votedNum++
					if votedNum > len(rf.peers)/2 {
						Log.Printf("[%v] After get %v number of votes, I'm Leader!\n", 
              rf.basicInfo(), votedNum)
						rf.setState(Leader)
            // reinitialized something after election
            newNextIndex := make([]int, len(rf.peers))
            lastLogIndex := rf.getLastLogIndex()
            for i := range newNextIndex {
              newNextIndex[i] = lastLogIndex + 1
            }
            rf.setNextIndex(newNextIndex)
            rf.setMatchIndex(make([]int, len(rf.peers)))
					}
				}
			}
			rf.mu.Unlock()
		default:
		}
	}

	done.Store(true)
	wg.Wait()
}

func (rf *Raft) doLeader() {
	replies := make(chan struct {
		int
		AppendEntriesReply
	}, len(rf.peers)-1)
	var wg sync.WaitGroup
	var done atomic.Bool
	done.Store(false)
	for i := 0; rf.getState() == Leader && rf.killed() == false &&
		!done.Load() && i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for rf.getState() == Leader && rf.killed() == false && !done.Load() {
        args := rf.NewAppendEntriesArgs(i)
				reply := AppendEntriesReply{}
				Log.Printf("[%v] try heartbeat from p%v\n", rf.basicInfo(), i)
				ok, isTimeout := rf.sendAppendEntries(i, &args, &reply)
				if rf.getState() != Leader {
					break
				}
				if ok {
					Assert(reply.ReceiverId == i, "")
					replies <- struct {
						int
						AppendEntriesReply
					}{args.Term, reply}
					Log.Printf("[%v] done AppendEntries(%v, %v) to %v\n",
						rf.basicInfo(), args, reply, i)
				} else if isTimeout {
					Log.Printf("[%v] call AppendEntries timeout\n", rf.basicInfo())
				} else {
					Log.Printf("[%v] call AppendEntries to %v fail\n", rf.basicInfo(), i)
				}
				time.Sleep(time.Duration(rand.Int63()%100+100) * time.Millisecond)
			}
		}(i)
	}
	for rf.getState() == Leader && rf.killed() == false {
		select {
		case reply := <-replies:
			rf.mu.Lock()
			if reply.int == rf.getTerm() {
				if reply.Term > rf.getTerm() {
					Log.Printf("[%v] cvt to follower by AppendEntried's Reply(%v) from p%v\n",
						rf.basicInfo(), reply.AppendEntriesReply, reply.ReceiverId)
					rf.setTerm(reply.Term)
					rf.setState(Follower)
					rf.setVotedFor(-1)
				}
			}

      rf.muLog.Lock()
      if reply.Success {
        rf.matchIndex[reply.ReceiverId] = rf.nextIndex[reply.ReceiverId]
        rf.nextIndex[reply.ReceiverId]++
      } else {
        if rf.nextIndex[reply.ReceiverId] > 0 {
          rf.nextIndex[reply.ReceiverId]--
        }
      }
      // update commitIndex and apply cmd to state machine
      for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
        if rf.log[i].Term == rf.getTerm() {
          nr_bigger := 0 
          for j := 0; j < len(rf.matchIndex); j++ {
            if j == rf.me {
              continue
            }
            if rf.matchIndex[j] > i {
              nr_bigger++
            }
          }
          if nr_bigger > len(rf.peers) / 2 {
            rf.commitIndex = i
            if rf.commitIndex > rf.lastApplied {
              // for simplicity just append one entry
              Assert(rf.commitIndex - rf.lastApplied == 1, "")
              rf.lastApplied = rf.commitIndex
              rf.applyCh <- ApplyMsg{
                true, rf.log[rf.lastApplied].Cmd, rf.lastApplied,
                false, nil, 0, 0}
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
	done.Store(true)
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
		if rf.getState() == Leader {
			rf.doLeader()
		} else if rf.timeout() {
			if rf.getState() == Follower {
				Log.Printf("[%v] follower election timeout\n", rf.basicInfo())
        rf.setState(Candidate)
				rf.election()
			} else if rf.getState() == Candidate {
				Log.Printf("[%v] candidate election timeout\n", rf.basicInfo())
				rf.election()
			}
		}

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
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
  rf.setCommitIndex(0)
  rf.setLastApplied(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	Log.Printf("Make raft peer %v done\n", rf.me)
	return rf
}

func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}

