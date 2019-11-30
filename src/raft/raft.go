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
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const HeartbeatInterval = 100 * time.Millisecond

type State uint8

const (
	FOLLOWER  = State(0)
	CANDIDATE = State(1)
	LEADER    = State(2)
)

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

const NULL int = -1

type AppendEntriesArgs struct {
	Term     int // leader的term
	LeaderId int // leader在节点数组中的下标
	//PrevLogIndex int        // index of log entry immediately preceding new ones, initially 0
	//PrevLogTerm  int        // term of prevLogIndex entry, initially -1
	//Entries      []LogEntry // log entries to store (empty for heartbeat; may
	// send more than one for efficiency)
	//LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	//nextIndex int // leader maintains a nextIndex for each follower, which is the index of next log
	// entry the leader will send to that follower
	// 1. If a follower does not have prevLogIndex in its log, it should return
	// with conflict-Index=len(log) and conflictTerm=None
	// 2. If a follower does have prevLogIndex in its log, but the term does not
	// match, it should return conflictTerm = log[prevLogIndex].Term, and search
	// its log for the index whose entry has term equal to conflictTerm
	// 3. Upon receiving a conflict response, the leader should first search its
	// log for conflictTerm. If it finds an entry in its log with that term, it
	// should set nextIndex to be the one beyond the index of the last entry
	// in that in its log
	// 4. If it does not find an entry with that term, it should set nextIndex = conflictIndex
	//ConflictIndex int
	//ConflictTerm  int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state State // 当前服务器的状态

	// Persistent state on all servers
	currentTerm int        // lastest term server has seen(initialized to 0 on first boot)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	votedFor    int        // 当前term给谁投票，如果没有就为null

	// electionTimeout
	electionTimer *time.Timer
	// heartbeatTimer
	heartbeatTimer *time.Timer
	// 重置electionTimer
	electionTimerResetChan chan bool

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be commited(initialized to 0,increase monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0,increase monotonically)

	// Volatile state on leaders: 每次选举后重新初始化
	nextIndex  []int // 发送给每个服务器的下一个log entry的index，初始化为leader的(last log index + 1)
	matchIndex []int // 对于每个服务器，index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	// 候选人获得的票数
	//voteAcquired int32

	// 日志需要提交时传递信息
	//commitCh chan interface{}

	// ApplyCh for ApplyMsg
	ApplyCh chan interface{}

	// 当候选人赢得了选举就会利用这个通道传送信息
	//leaderCh chan interface{}

	//shutdownChan chan bool

	heartbeatResetChan chan bool
}

// helper function
func (rf *Raft) getRandomElectionTimeOut() time.Duration {
	//return time.Duration(rand.Int63()%333+550) * time.Millisecond
	return time.Duration(rand.Int63()%150+300) * time.Millisecond
}
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (s State) String() string {
	switch {
	case s == FOLLOWER:
		return "Follower"
	case s == CANDIDATE:
		return "Candidate"
	case s == LEADER:
		return "Leader"
	}
	return "Unknown State"
}

/*
func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIndex()].LogTerm
}

func (rf *Raft) getLogLen() int {
	return len(rf.log) - 1 //因为第一项由0开始
}
func (rf *Raft) getPrevLogIndex(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	return rf.log[rf.getPrevLogIndex(i)].LogTerm
}
*/

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("readPersist failure: [server:%d]", rf.me)
	} else {
		rf.currentTerm, rf.votedFor, rf.log = currentTerm, votedFor, log
	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int // 候选人在节点数组中的下标
	//LastLogIndex int // 候选人的最后一个log entry的下标
	//LastLogTerm  int // 候选人的最后一个log entry的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means 候选人收到投票

}

func (rf *Raft) beCandidate() {
	// 候选人要准备投票
	//rf.mu.Lock()
	rf.currentTerm++
	// 先投给自己
	rf.votedFor = rf.me
	//rf.voteAcquired = 1
	rf.state = CANDIDATE
	//rf.persist()
	//rf.mu.Unlock()
}

func (rf *Raft) beFollower() {
	//rf.mu.Lock()
	rf.state = FOLLOWER
	rf.votedFor = -1

	//rf.mu.Unlock()
}

func (rf *Raft) beLeader() {
	//rf.mu.Lock()
	rf.mu.Lock()
	rf.state = LEADER

	// nextIndex和matchIndex是两个只有leader用的数组，对于它们进行初始化
	// 细节可看此：https://www.cs.princeton.edu/courses/archive/fall18/cos418/docs/p7-raft-no-solutions.pdf
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = 0

	}

	rf.matchIndex[rf.me] = rf.getLastIndex()
	rf.mu.Unlock()
	DPrintf("printf say something %v", 27)
	go func() {
		rf.heartbeatResetChan <- true
		DPrintf("printf say something %v", 26)
	}()

	//rf.mu.Unlock()
}

// Receiver's implementation
// (1). VoteGranted回复false，如果Term < currentTerm
// (2). VoteGranted回复true，如果voteFor是null或者候选人Id，并且候选人的LastLogTerm >=  接收者的currentTerm,
//
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		DPrintf("%v Helloworlf", 13)
		reply.Term = rf.currentTerm

	} else {
		// DPrintf("%v Helloworlf", 14)
		// args.Term >= rf.currentTerm

		/*reply.Term = args.Term
		// if is null or candidateId (candidate itself)
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			// 检查 候选人的log是不是at least as up-to-date
			lastLogIndex := rf.getLastIndex()
			lastLogTerm := rf.getLastTerm()

			if (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) || args.LastLogTerm > lastLogTerm {
				//rf.beFollower()
				reply.VoteGranted = true
				rf.currentTerm = args.Term
				rf.votedFor = args.CandidateId
				//rf.electionTimerResetChan <- true
				rf.convertTo(FOLLOWER)

			}

		}*/
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.convertTo(FOLLOWER)
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// AppendEntriesRPC接收者实现
// 1. if term < currentTerm , 回复false
// 2. Reply false if log doesn't contain an entry at prevLogIndex
//  	whose term matches prevLogTerm
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
// AppendEntries
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()

	defer rf.mu.Unlock()

	//defer rf.persist()
	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	reply.Success = true
	reply.Term = args.Term
	//rf.electionTimerResetChan <- true
	rf.convertTo(FOLLOWER)
	return
}

// sendAppendEntries
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) broadcastAppendEntries() {

	//N := rf.commitIndex
	//last := rf.getLastIndex()
	//rf.updateLastCommit()
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	for j := range rf.peers {
		if j != rf.me && rf.state == LEADER {
			go func(idx int) {
				//rf.mu.Lock()

				//args.PrevLogIndex = rf.getPrevLogIndex(idx)
				//args.PrevLogTerm = rf.getPrevLogTerm(idx)
				//args.LeaderCommit = rf.commitIndex
				//if rf.nextIndex[idx] != rf.getLastIndex() && rf.matchIndex[idx]+1 == rf.nextIndex[idx] {
				//	args.Entries = rf.log[rf.matchIndex[idx]:]
				//}
				//rf.mu.Unlock()
				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(idx, args, reply)

				if !ok {
					DPrintf("server[%d]'s sendAppendEntries failure", idx)
					return
				}
				// 比较leader的term是否过时
				rf.mu.Lock()

				if reply.Term > rf.currentTerm && rf.state == LEADER {
					rf.currentTerm = reply.Term
					DPrintf("Leader outdated")
					rf.beFollower()
					DPrintf("Leader outdated cannot be ")
					return
				}

				//rf.nextIndex[idx] = reply.ConflictIndex

				//DPrintf("printf say something %v", 25)
				//if !reply.Success && rf.state == LEADER {
				//	for i := rf.getLastIndex(); i >= 0; i-- {
				//		if rf.log[i].LogTerm == reply.ConflictTerm {
				//			rf.nextIndex[idx] = i
				//			return
				//		}
				//	}
				//}
				rf.mu.Unlock()

			}(j)
		}

	}
}

func (rf *Raft) broadcastRequestVote() {
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	// 候选人获得的票数
	var voteAcquired int32

	for i := range rf.peers {
		if i == rf.me {
			rf.votedFor = rf.me
			atomic.AddInt32(&voteAcquired, 1)
			continue
		}

		go func(idx int) {
			// reply := &RequestVoteReply{}
			var reply RequestVoteReply
			if rf.sendRequestVote(idx, &args, &reply) {
				rf.mu.Lock()

				if reply.VoteGranted && rf.state == CANDIDATE {
					atomic.AddInt32(&voteAcquired, 1)
					if atomic.LoadInt32(&voteAcquired) > int32(len(rf.peers)/2) {
						rf.convertTo(LEADER)
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(FOLLOWER)
					}
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				DPrintf("sever %v send request vote to server %d failed", rf.me, idx)
				rf.mu.Unlock()
			}
		}(i)
	}
}

/*
func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	rf.mu.Unlock()

	for i := range rf.peers {
		//DPrintf("%v Helloworlf", 11)
		if i != rf.me && rf.state == CANDIDATE {
			// DPrintf("%v Helloworlf", 10)
			go func(idx int) {
				reply := &RequestVoteReply{}

				rf.sendRequestVote(idx, &args, reply)
				rf.mu.Lock()

				if reply.Term > rf.currentTerm && rf.state == CANDIDATE {
					rf.currentTerm = reply.Term
					rf.beFollower()
					DPrintf("%v Helloworlf become a follower", 9)
					return
				}

				if reply.VoteGranted == true {
					atomic.AddInt32(&rf.voteAcquired, 1)
				}
				rf.mu.Unlock()
				if atomic.LoadInt32(&rf.voteAcquired) > int32(len(rf.peers)/2) && rf.state == CANDIDATE {
					DPrintf("A new leader")

					rf.beLeader()
					DPrintf("%v Helloworlf", 99)
				}
			}(i)
		}
	}
	rf.mu.Lock()
	DPrintf("printf say something wrong")
	rf.heartbeatResetChan <- true
	DPrintf("printf say something right")
	rf.mu.Unlock()

}
*/

// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
func (rf *Raft) updateLastApplied() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		command := rf.log[rf.lastApplied].Command
		applyMsg := ApplyMsg{true, command, rf.lastApplied}
		rf.ApplyCh <- applyMsg
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
func (rf *Raft) updateLastCommit() {
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)

	sort.Sort(sort.Reverse(sort.IntSlice(matchIndexCopy)))
	N := matchIndexCopy[len(matchIndexCopy)/2]
	if N > rf.commitIndex && rf.log[N].LogTerm == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}

}

func (rf *Raft) convertTo(nodestate State) {
	//if nodestate == rf.state {
	//	return
	//}
	if nodestate == CANDIDATE {
		rf.currentTerm += 1
	}
	DPrintf("Term %d: server %d convert from %v to %v\n",
		rf.currentTerm, rf.me, rf.state, nodestate)
	rf.state = nodestate
	switch nodestate {
	case FOLLOWER:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(rf.getRandomElectionTimeOut())
		//rf.votedFor = -1

	case CANDIDATE:
		rf.votedFor = rf.me
		rf.electionTimer.Reset(rf.getRandomElectionTimeOut())
		rf.broadcastRequestVote()

	case LEADER:
		rf.electionTimer.Stop()
		rf.broadcastAppendEntries()
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers

	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = []LogEntry{{0, 0, nil}} // first index is 1, first term  is also 1 , {LogIndex, LogTerm, Command}
	rf.currentTerm = 0

	//rf.leaderCh = make(chan interface{})
	//rf.commitCh = make(chan interface{})

	//rf.electionTimerResetChan = make(chan bool)
	//rf.heartbeatResetChan = make(chan bool)
	// nextIndex和matchIndex只有leader用
	// rf.nextIndex = make([]int, len(peers))
	// rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())

	// Modify Make() to create a background goroutine that will kick off leader election
	// periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	// This way a peer will learn who is the leader, if there is already a leader, or become the leader itself.

	rf.electionTimer = time.NewTimer(rf.getRandomElectionTimeOut())
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	go func() {

		for {
			select {

			case <-rf.electionTimer.C:
				rf.mu.Lock()
				if rf.state == FOLLOWER || rf.state == CANDIDATE {
					rf.convertTo(CANDIDATE)
				} //else {
				//	rf.broadcastRequestVote()
				//}
				rf.mu.Unlock()

			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == LEADER {
					rf.broadcastAppendEntries()
					rf.heartbeatTimer.Reset(HeartbeatInterval)
				}
				rf.mu.Unlock()
			}
		}
	}()

	/*
		go func() {

			rf.electionTimer = time.NewTimer(rf.getRandomElectionTimeOut())

			for {
				select {
				case <-rf.electionTimerResetChan:
					rf.mu.Lock()
					if rf.state == CANDIDATE || rf.state == FOLLOWER {
						rf.electionTimer.Reset(rf.getRandomElectionTimeOut())
					}
					rf.mu.Unlock()

				case <-rf.electionTimer.C:

					rf.mu.Lock()
					if rf.state == CANDIDATE || rf.state == FOLLOWER {

						rf.beCandidate()
						//DPrintf("%v HelloworldCandidate", 10)
						rf.mu.Unlock()

						go rf.broadcastRequestVote()
						go func() { rf.electionTimerResetChan <- true }()
						DPrintf("%v", 22)

					}
				case <-rf.heartbeatResetChan:
					time.Sleep(100 * time.Millisecond)
					//

					rf.mu.Lock()
					DPrintf("%v Helloworlf", 9)
					if rf.state == LEADER {
						DPrintf("printf say something %v", 35)
						rf.mu.Unlock()
						go rf.broadcastAppendEntries()
						DPrintf("printf say something %v", 36)
					}
				case <-rf.shutdownChan:
					if !rf.electionTimer.Stop() {
						<-rf.electionTimer.C
					}
					close(rf.electionTimerResetChan)
					close(rf.shutdownChan)
					return
				}

			}
		}()
	*/
	return rf
}
