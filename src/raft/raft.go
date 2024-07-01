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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
    "fmt"
	"6.5840/labrpc"
)

const ElectionTimeout = 200 * time.Millisecond


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

type State int
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

	// Your data here (3A, 3B, 3C).
    // if one server’s current term is smaller than the other’s, 
    //   then it updates its current term to the larger value. 
    // If a candidate or leader discovers that its term is out of date, 
    //   it immediately reverts to follower state. 
    // If a server receives a request with a stale term number, it rejects the request
    currentTerm int
    // candidateId that received vote in current term (or null if none)
    votedFor int
    // Server states. 
    // Followers only respond to requests from other servers.  
    //   If a follower receives no communication, it becomes a candidate and initiates an election. 
    // A candidate that receives votes from a majority of the full cluster becomes the new leader. 
    // Leaders typically operate until they fail.
    currentState State
    lastTime time.Time // last time out
    timeoutVal int64 // milliseconds
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == Leader
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
    // lastLogIndex int
    // lastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
    /* currentTerm, for candidate to update itself */
    Term int
    /* true means candidate received vote */
    VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.lastTime = time.Now()
    rf.setTimeoutVal()

    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    if args.Term < rf.currentTerm {
        return
    } else if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term // INFO Page4, Rules for Servers
        rf.votedFor = -1
        rf.currentState = Follower
    }

    // INFO page6, "first-come-first-served" basis
    if rf.votedFor == -1 {
        reply.VoteGranted = true
        Log.Printf("[%v] give p%v vote\n", rf.basicInfo(), args.CandidateId)
        rf.votedFor = args.CandidateId
    }
}

type AppendEntriesArgs struct {
    /* leader's term */
    Term int
    LeaderId int
    // ...
}

type AppendEntriesReply struct {
    /* currentTerm, for leader to update itself */
    Term int
    /* true if follower contained entry matching prevLogIndex and prevLogTerm */
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.lastTime = time.Now()
    rf.setTimeoutVal()

    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        reply.Success = false
        return
    } else if args.Term == rf.currentTerm {
        reply.Success = true
    } else {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.currentState = Follower
    }
    reply.Success = true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


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
    rf.mu.Lock()
    rf.currentTerm++
    rf.lastTime = time.Now()
    rf.setTimeoutVal()
    rf.votedFor = rf.me
    votedNum := 1 
    rf.mu.Unlock()

    // While waiting for votes, a candidate may receive an
    // AppendEntries RPC from another server claiming to be
    // leader. which may let the candidate become follower state. 
    replies := make(chan RequestVoteReply, len(rf.peers) - 1)
    for i := 0; rf.killed() == false && rf.currentState == Candidate &&
                i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        go func(i int) {
            args := RequestVoteArgs{rf.currentTerm, rf.me}
            reply := RequestVoteReply{}
            for rf.killed() == false && rf.currentState == Candidate {
                Log.Printf("[%v] try vote from p%v\n", rf.basicInfo(), i)
                ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)
                if ok {
                    replies <- reply
                    Log.Printf("[%v] done RequestVote(%v, %v)\n", rf.basicInfo(), args, reply)
                    break
                } else {
                    Log.Printf("[%v] call RequestVote to %v fail\n", rf.basicInfo(), i)
                }
            }
        } (i)

    }
    for rf.killed() == false && rf.currentState == Candidate {
        select {
        case reply := <-replies:
            Log.Printf("[%v] reply is %v\n", rf.basicInfo(), reply) 
            rf.mu.Lock()
            if reply.Term > rf.currentTerm {
                rf.currentTerm = reply.Term
                rf.currentState = Follower
                rf.mu.Unlock()
                break
            }
            if reply.VoteGranted {
                votedNum++
                if votedNum > len(rf.peers) / 2 {
                    Log.Printf("[%v] After get %v number of votes, I'm Leader!\n", rf.basicInfo(), votedNum)
                    rf.currentState = Leader
                    rf.mu.Unlock()
                    break
                }
            }
            rf.mu.Unlock()
        }
    }
}

func (rf *Raft) timeout() bool {
    return time.Now().Sub(rf.lastTime).Milliseconds() > rf.timeoutVal
}

func (rf *Raft) basicInfo() string {
    return fmt.Sprintf("p%v-t%v-%v", rf.me, rf.currentTerm, rf.currentState)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
        // Log.Printf("[%v] ticker loop start\n", rf.basicInfo())
		// Your code here (3A)
		// Check if a leader election should be started.
        if rf.currentState == Leader {
            replies := make(chan AppendEntriesReply, len(rf.peers) - 1)
            for i := 0; rf.currentState == Leader && rf.killed() == false && 
                        i < len(rf.peers); i++ {
                if i == rf.me {
                    continue
                }
                go func (i int) {
                    for rf.currentState == Leader && rf.killed() == false {
                        args := AppendEntriesArgs{rf.currentTerm, rf.me}
                        reply := AppendEntriesReply{}
                        Log.Printf("[%v] heartbeat from p%v\n", rf.basicInfo(), i)
                        ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
                        if ok {
                            Log.Printf("[%v] done AppendEntries(%v, %v)\n", rf.basicInfo(), args, reply)
                            replies <- reply
                        } else {
                            Log.Printf("[%v] call AppendEntries to %v fail\n", rf.basicInfo(), i)
                        }
                    }
                } (i)
            }
            for rf.currentState == Leader && rf.killed() == false {
                select {
                case reply := <-replies:
                    if reply.Term > rf.currentTerm {
                        rf.mu.Lock()
                        rf.currentTerm = reply.Term
                        rf.currentState = Follower
                        rf.votedFor = -1
                        rf.mu.Unlock()
                    }
                }
            }
        } else if rf.timeout() {
            if rf.currentState == Follower {
                Log.Printf("[%v] follower election timeout\n", rf.basicInfo())
                rf.mu.Lock()
                rf.currentState = Candidate
                rf.mu.Unlock()
                rf.election()
            } else if rf.currentState == Candidate {
                Log.Printf("[%v] candidate election timeout\n", rf.basicInfo())
                rf.election()
            }
        }

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 10 + (rand.Int63() % 10)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) setTimeoutVal() {
    rf.timeoutVal = rand.Int63() % 150 + 500 
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

	// Your initialization code here (3A, 3B, 3C).
    rf.lastTime = time.Now()
    rf.setTimeoutVal()
    rf.currentState = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

    Log.Printf("Make raft peer %v done\n", rf.me)
	return rf
}

