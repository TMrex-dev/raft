package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"time"

	"sync"
	"sync/atomic"

	"cs350/labgob"
	"cs350/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // This peer's index into peers[]
	dead          int32               // Set by Kill()
	CurrentTerm   int                 //latest term server has seen
	VotedFor      int                 //candidateID that received vote in currentTerm
	Log           []LogEntry          //log entries
	CommitIndex   int                 //index of highest log entry to be commited
	lastApplied   int                 //index of highest log entry applied to state machine
	nextIndex     map[int]int         //index of next log entry to be sent to server
	matchIndex    map[int]int         //index of highest log entry known to be replicated on server
	IsLeader      bool                //boolean for whether or not server is leader
	State         string              //boolean for whether or not server is candidate
	TimeStamp     time.Time
	ElectionTimer time.Duration
	VotesGotten   int
	applyMsg      chan ApplyMsg

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// should be mostly done
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = rf.IsLeader

	// Your code here (4A).

	//maybe done

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	//sure??? maybe this is it????
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (4B).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		// Handle decoding error
		panic("bad thang")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		// rf.persist()
	}
	//probably done
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int

	//possibly correct
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term        int
	VoteGranted bool

	//possibly correct
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if term < current term return false

	//log.Println(rf.me, "got a command with prev log index", args.PrevLogIndex, "with entries:", args.Entries)

	//log.Println("we are length", len(rf.Log))

	//log.Println(rf.me, "follower log before doing anything looks like", rf.Log)

	//log.Println(args.LeaderId, "gave these entries to append", args.Entries)

	if args.Term < rf.CurrentTerm {
		log.Println("step 1", rf.me, "replied false to", args.LeaderId, "who has term", args.Term, "because", rf.me, "has term", rf.CurrentTerm)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	} //step 1

	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term)
	} //the become follower logic

	if len(rf.Log) <= args.PrevLogIndex {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.ConflictIndex = len(rf.Log)
		reply.ConflictTerm = 0
		return
	} //If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		log.Println("step 2", rf.me, "replied false")
		log.Println("len of log", rf.me, "is", len(rf.Log), "len of argsIndex", args.PrevLogIndex+1)
		log.Println(rf.me, "previouslogindexterm", "leader prev log term", args.PrevLogTerm)
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term

		for index := range rf.Log {
			if rf.Log[index].Term == reply.ConflictTerm {
				reply.ConflictIndex = index
				return
			}
		}
		return
	} //step 2, if len of log is less than prev index than its outdated, if the term at log of prev index isn't the same than its also outdated

	// if len(rf.Log) > args.PrevLogIndex+1 && rf.Log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
	// 	rf.Log = rf.Log[:args.PrevLogIndex+1]
	// 	rf.persist()
	// } //step 3
	for i := range args.Entries {
		entryIndex := args.PrevLogIndex + 1 + i
		//log.Println("entryIndex is,", entryIndex)
		if len(rf.Log) > entryIndex && rf.Log[entryIndex].Term != args.Entries[i].Term {
			rf.Log = rf.Log[:entryIndex]
			rf.persist()
		}
		if len(rf.Log) <= entryIndex {
			//log.Println("log before is", rf.Log)
			//log.Println("the entry is", args.Entries[i])
			rf.Log = append(rf.Log, args.Entries[i])
			rf.persist()
			// log.Println("log after is", rf.Log)
		}
	} //step 3 & 4

	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit > len(rf.Log)-1 {
			rf.CommitIndex = len(rf.Log) - 1
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
		log.Println(rf.me, "commit index is", rf.CommitIndex)
	} //step 5

	//log.Println(rf.me, "follower log after looks like", rf.Log)

	// Respond to the heartbeat
	rf.TimeStamp = time.Now()
	reply.Term = rf.CurrentTerm
	reply.Success = true

}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Println(rf.me, "received a vote request from", args.CandidateID, "who has term", args.Term)
	//don't grant vote if term is less than currentterm

	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm {

		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		log.Println(rf.me, "denied vote to", args.CandidateID, "in term", rf.CurrentTerm)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.becomeFollower(args.Term)
	}

	//log.Println(rf.VotedFor)
	//log.Println(args.CandidateID)
	//don't grant vote if votedfor is not null or the candidate id

	candidateUpToDate := false
	if args.LastLogTerm > rf.Log[len(rf.Log)-1].Term ||
		(args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex >= len(rf.Log)-1) {
		candidateUpToDate = true
	}

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && candidateUpToDate {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
		rf.persist()
		reply.Term = rf.CurrentTerm
		rf.TimeStamp = time.Now()

		log.Println(rf.me, "granted vote to", args.CandidateID, "in term", rf.CurrentTerm)
		return
	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

}

func (rf *Raft) becomeFollower(term int) {
	rf.CurrentTerm = term
	rf.IsLeader = false
	rf.VotedFor = -1
	rf.persist()
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (4B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Println(rf.me, "got this command", command)

	if !rf.IsLeader {
		return index, term, false
	}

	logEntry := LogEntry{
		Term:    rf.CurrentTerm,
		Command: command,
	}

	rf.Log = append(rf.Log, logEntry) //leader rule 2

	//log.Println(rf.me, "appended command so now it looks like", rf.Log)

	rf.persist()

	index = len(rf.Log) - 1 // this is correct, it's the index of the most recent entry

	term = rf.CurrentTerm

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep()

		rf.mu.Lock()

		if !rf.IsLeader {
			if time.Since(rf.TimeStamp) >= rf.ElectionTimer {
				rf.startElection()
			}
		} else {
			for N := rf.CommitIndex + 1; N < len(rf.Log); N++ {
				if rf.Log[N].Term == rf.CurrentTerm {
					count := 1
					for server := range rf.peers {
						if server != rf.me && rf.matchIndex[server] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.CommitIndex = N
						log.Println("leader", rf.me, "set thier commit index to", rf.CommitIndex)
					}
				}
			}
		}
		for rf.CommitIndex > rf.lastApplied {
			rf.lastApplied++

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyMsg <- applyMsg

			log.Println(rf.me, "committed", rf.lastApplied)

		}

		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {

	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.TimeStamp = time.Now()
	rf.VotesGotten = 1
	rf.persist()

	log.Println(rf.me, "started an election")

	for server := range rf.peers {
		if server != rf.me {
			go func(server int) {
				rf.mu.Lock()

				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateID:  rf.me,
					LastLogIndex: len(rf.Log) - 1,
					LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
				}
				reply := RequestVoteReply{}
				rf.mu.Unlock()

				rf.sendRequestVote(server, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.CurrentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.VoteGranted && !rf.IsLeader {
					rf.VotesGotten++
					if rf.VotesGotten > len(rf.peers)/2 {
						log.Println(rf.me, "is now a leader with", rf.VotesGotten, "votes", "and term", rf.CurrentTerm)
						rf.IsLeader = true
						for server := range rf.peers {
							rf.nextIndex[server] = len(rf.Log) //last log index + 1
							log.Println("length of leader log", len(rf.Log))
							rf.matchIndex[server] = -1
						}

						go rf.sendHeartbeats()
					}
				}

			}(server)
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	for {
		rf.mu.Lock()
		if !rf.IsLeader {
			rf.mu.Unlock()
			return
		}

		// log.Println("leader match index", rf.matchIndex)
		// log.Println("leader next Index", rf.nextIndex)

		for server := range rf.peers {
			if server != rf.me {
				if len(rf.Log)-1 >= rf.nextIndex[server] {
					go func(server int) {
						//log.Println("case with entries")
						rf.mu.Lock()

						// log.Println("the lenght of log is", len(rf.Log), "it looks like", rf.Log)
						// log.Println("next index value is", rf.nextIndex[server])
						// log.Println("matchIndex Value is", rf.matchIndex[server])
						// log.Println("this value is", rf.Log[rf.nextIndex[server]-1].Term)

						args := AppendEntriesArgs{
							Term:         rf.CurrentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[server] - 1,
							PrevLogTerm:  rf.Log[rf.nextIndex[server]-1].Term,
							Entries:      rf.Log[rf.nextIndex[server]:],
							LeaderCommit: rf.CommitIndex,
						}

						reply := AppendEntriesReply{}

						rf.mu.Unlock()

						//log.Println("I sent stuff")
						rf.sendAppendEntries(server, &args, &reply)

						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.CurrentTerm {
							rf.becomeFollower(reply.Term)
							return
						}

						//log.Println(reply.Success)

						if reply.Success {
							//volatile state on leaders
							rf.nextIndex[server] = len(rf.Log) //index of next log entry to send to server
							rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

							//log.Println("next index value after success", rf.nextIndex[server])
							//log.Println("match index value after success", rf.matchIndex[server])
							return

						} else {
							//log.Println("we are decreasing stuff")
							for entryIndex := range rf.Log {
								if rf.Log[entryIndex].Term == reply.ConflictTerm {
									rf.nextIndex[server] = entryIndex + 1
									return
								}
							}

							rf.nextIndex[server] = reply.ConflictIndex

							//if AppendEntries fails because of log inconsistency: decrement nextIndex and retry
						}

						//log.Println("I think we are stuck in this for loop")

					}(server)
				} else {
					go func(server int) {
						rf.mu.Lock()

						args := AppendEntriesArgs{
							Term:         rf.CurrentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[server] - 1,
							PrevLogTerm:  rf.Log[rf.nextIndex[server]-1].Term,
							Entries:      nil,
							LeaderCommit: rf.CommitIndex,
						}

						reply := AppendEntriesReply{}

						rf.mu.Unlock()
						rf.sendAppendEntries(server, &args, &reply)

						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.CurrentTerm {
							rf.becomeFollower(reply.Term)
							return
						}

						//log.Println(reply.Success)

						if reply.Success {
							//volatile state on leaders
							rf.nextIndex[server] = len(rf.Log) //index of next log entry to send to server
							rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

							//log.Println("next index value after success", rf.nextIndex[server])
							//log.Println("match index value after success", rf.matchIndex[server])
							return
						} else {
							for entryIndex := range rf.Log {
								if rf.Log[entryIndex].Term == reply.ConflictTerm {
									rf.nextIndex[server] = entryIndex + 1
									return
								}
							}

							rf.nextIndex[server] = reply.ConflictIndex
						}

						//log.Println("I think we are stuck in this for loop")

					}(server)
				}

			}
		}

		rf.mu.Unlock()

		time.Sleep(150 * time.Millisecond)
	}

}

func (rf *Raft) getElectionTimer() time.Duration {
	duration := time.Duration(rand.Intn(500)+300) * time.Millisecond
	return duration
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (4A, 4B).

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.VotesGotten = 0
	rf.CommitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.IsLeader = false
	rf.Log = make([]LogEntry, 1)

	rf.TimeStamp = time.Now()
	rf.ElectionTimer = rf.getElectionTimer()
	rf.applyMsg = applyCh

	// initialize from state persisted before a crash.

	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections.

	go rf.ticker()

	rf.mu.Unlock()

	return rf
}
