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
	"labgob"
	"labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"

	"sync"
	"sync/atomic"
	"time"
)

const (
	HeartbeatInterval = 100 * time.Millisecond
	MaxLockTimeTime   = 10 * time.Millisecond
)

type State uint8

const (
	FOLLOWER  = State(0)
	CANDIDATE = State(1)
	LEADER    = State(2)
)

type LogEntry struct {
	// LogIndex int
	LogTerm int
	Command interface{}
}

const NULL int = -1

type AppendEntriesArgs struct {
	Term     int // leader的term
	LeaderId int // leader在节点数组中的下标

	// 2B
	PrevLogIndex int        // index of log entry immediately preceding new ones, initially 0
	PrevLogTerm  int        // term of prevLogIndex entry, initially -1
	Entries      []LogEntry // log entries to store (empty for heartbeat; may
	// send more than one for efficiency)
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	//nextIndex int // leader maintains a nextIndex for each follower, which is the index of next log
	// entry the leader will send to that follower
	// 1. If a follower does not have prevLogIndex in its log, it should return
	// with conflict-Index=self.LastLogIndex+ 1 and conflictTerm=None
	// 2. If a follower does have prevLogIndex in its log, but the term does not
	// match, it should return conflictTerm = log[prevLogIndex].Term, and search
	// its log for the index whose entry has term equal to conflictTerm 第一个term为conflictTerm的conflictIndex
	// 3. Upon receiving a conflict response, the leader should first search its
	// log for conflictTerm. If it finds an entry in its log with that term, it
	// should set nextIndex to be the one beyond the index of the last entry
	// in that in its log
	// 4. If it does not find an entry with that term, it should set nextIndex = conflictIndex
	ConflictIndex int // 2C
	ConflictTerm  int // 2C
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
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1) 当时实现时候有什么问题吗？查bug
	votedFor    int        // 当前term给谁投票，如果没有就为null

	// electionTimeout
	electionTimer *time.Timer
	// heartbeatTimer
	heartbeatTimer *time.Timer

	stopCh chan struct{}

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be commited(initialized to 0,increase monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0,increase monotonically)

	// Volatile state on leaders: 每次选举后重新初始化
	nextIndex  []int // 发送给每个服务器的下一个log entry的index，初始化为leader的(last log index + 1)
	matchIndex []int // 对于每个服务器，index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	// ApplyCh for ApplyMsg
	applyCh chan ApplyMsg

	// hope for accelerate it
	notifyApplyCh chan struct{}

	// for debug
	lockStart        time.Time
	lockEnd          time.Time
	lockMethodName   string
	unlockMethodName string
	dead             int32
}

// helper functions below
// 选举超时区间：[256-512)ms
func (rf *Raft) getRandomElectionTimeOut() time.Duration {
	//return time.Duration(rand.Int63()%333+550) * time.Millisecond
	return time.Duration(rand.Int63()%256+300) * time.Millisecond
}
func (rf *Raft) lock(method string) {
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockMethodName = method
}

func (rf *Raft) unlock(method string) {
	rf.lockEnd = time.Now()
	rf.unlockMethodName = method
	duration := rf.lockEnd.Sub(rf.lockStart)
	if duration > MaxLockTimeTime {
		DPrintf("the lock operation in method %s, and unlock in method %s lock for a while "+
			"whose duration is %v, and the server[%v] is dead[%v]\n", rf.lockMethodName, rf.unlockMethodName,
			duration, rf.me, rf.killed())
	}
	rf.mu.Unlock()
}

// better way to stop timer
func (rf *Raft) stop(timer *time.Timer) {
	if !timer.Stop() && len(timer.C) != 0 {
		<-timer.C
	}
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

func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIndex()].LogTerm
}

func (rf *Raft) getLogLen() int {
	return len(rf.log) - 1 //因为第一项由1开始
}
func (rf *Raft) getPrevLogIndex(i int) int {
	if rf.nextIndex[i]-1 == -1 {
		fmt.Printf("something wrong in PrevLogIndex[%v] in server[%v]", i, rf.me)
	}

	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	return rf.log[rf.getPrevLogIndex(i)].LogTerm
}

// above are all helper methods

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.lock("GetState()")
	defer rf.unlock("GetState()")
	term = rf.currentTerm
	isleader = rf.state == LEADER
	// DPrintf("the [%d]raft's term is [%d]", rf.me, term)
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
	t1 := time.Now()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	duration := time.Now().Sub(t1)
	DPrintf("persist time finish in [%v]!!!\n", duration)
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
		panic("readPersist failure : [server]")
		// DPrintf("readPersist failure: [server:%d]", rf.me)
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

	// 2B
	LastLogIndex int // 候选人的最后一个log entry的下标
	LastLogTerm  int // 候选人的最后一个log entry的term
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

// Receiver's implementation
// (1). VoteGranted回复false，如果Term < currentTerm
// (2). VoteGranted回复true，如果voteFor是null或者候选人Id，并且候选人的LastLogTerm >=  接收者的currentTerm,
//
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.lock("RequestVote()")
	// defer rf.unlock("RequestVote()")
	// defer rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		rf.unlock("RequestVote-1")
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.unlock("RequestVote-2")
			return

		} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			// 已经投给其他人了
			rf.unlock("RequestVote-2")
			return

		} else if rf.state == LEADER { // 我已经是这个term的leader了，不会给任何人投票
			rf.unlock("RequestVote-3_leader")
			return
		}

	}
	var need_persist = false
	// args's term 比它大 即比它up-to-date
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == LEADER {
			// rf.heartbeatTimer.Stop()
			rf.stop(rf.heartbeatTimer)
			DPrintf("the leader[%v] convert to follower in function RequestVote\n", rf.me)
			// rf.electionTimer.Stop()
			rf.stop(rf.electionTimer)
			rf.electionTimer.Reset(rf.getRandomElectionTimeOut())

		} else if rf.state == CANDIDATE {
			// 候选人转为follower这里，需要重设时间吗
			DPrintf("the candidate[%v] convert to follower in function RequestVote\n", rf.me)
			// rf.electionTimer.Stop()
			// rf.electionTimer.Reset(rf.getRandomElectionTimeOut())

		}
		rf.convertTo(FOLLOWER)
		need_persist = true
		// rf.persist()
	}

	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	// 如果两个候选者，一个term比另一个大，需要给他投票
	// 如果一个候选者，和一个投票了的follower， 候选者term更大，follower状态需要更新，并且给他投票
	// 如果是一个没投票的follower它需要更新状态来投票吗？
	// 明确一点投票时，是给与自己的最大term一样的服务器同时自己还未投票的情况下给其投票
	// more up-to-date才给投票
	// 就是说一个follower哪怕没投票只要比它up-to-date就不要投票给他
	// 也只有投票了才需要重置时间
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// for i := range rf.log {
		// DPrintf("server[%d]'s in term[%v] rf.log is %v", rf.me, rf.currentTerm, rf.log[i])
		// }
		if lastTerm < args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			// rf.persist()

			// 需要重置时间在这里,只有给予投票才重置时间嘛？
			// rf.electionTimer.Stop()
			rf.stop(rf.electionTimer)
			rf.electionTimer.Reset(rf.getRandomElectionTimeOut())
			DPrintf("the server[%v] give the server[%v] a vote in RequestVote at the time[%v]\n",
				rf.me, args.CandidateId, time.Now())
			need_persist = true
		}

	}

	if need_persist {
		rf.persist()
	}

	rf.unlock("RequestVote-3")
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
	isLeader := false

	// Your code here (2B).
	rf.lock("Start")
	// term, isLeader = rf.GetState()
	isLeader = rf.state == LEADER
	term = rf.currentTerm
	if isLeader {
		// rf.lock("Start")
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{LogTerm: term, Command: command})

		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1

		rf.persist()
		// rf.unlock("Start")
		DPrintf("Leader[%v]提交Start日志,它的LogTerm是[%v]和LogCommand[%v]\n", rf.me,
			term, command)
	}
	rf.unlock("Start")
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

func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool {
	AElastIndex := args.PrevLogIndex + len(args.Entries)
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()

	if AElastIndex < lastLogIndex && args.Term == lastLogTerm {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.lock("AppendEntries")
	DPrintf("server[%v]进入AppendEntries, 来自leader[%v], leader的term是[%v]\n", rf.me, args.LeaderId, args.Term)
	//defer rf.persist()
	//defer rf.persist()
	// 1. Reply false if term < currentTerm
	// 只有这种情况不需要重置electionTimer，其他均需要
	reply.Term = rf.currentTerm // currentTerm, for leader to update itself

	if args.Term < rf.currentTerm {
		reply.Success = false

		DPrintf("server[%v] 在 AppendEntries失败了, 由于leader[%v]的term[%v]比当前term[%v]小\n", rf.me,
			args.LeaderId, args.Term, rf.currentTerm)
		rf.unlock("AppendEntries")
		return
	}
	var need_persist = false
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		if rf.state == LEADER {
			// rf.heartbeatTimer.Stop()
			rf.stop(rf.heartbeatTimer)
			DPrintf("the leader[%v] convert to follower in the function AppendEntries\n", rf.me)
		} else if rf.state == CANDIDATE {
			rf.votedFor = -1
		}
		need_persist = true
	} else if rf.currentTerm == args.Term {
		if rf.state == CANDIDATE {
			rf.votedFor = -1
			need_persist = true
		}
	}
	rf.convertTo(FOLLOWER)
	// rf.electionTimer.Stop()
	rf.stop(rf.electionTimer)
	rf.electionTimer.Reset(rf.getRandomElectionTimeOut()) // 收到心跳包也需要重置选举时间

	// rf.persist()
	// 1. If a follower does not have prevLogIndex in its log, it
	// should return with conflictIndex = self.lastLogIndex + 1 and conflictTerm = None
	// 2. If a follower does have prevLogIndex in its log, but the term does not match, it
	// should return conflictTerm = log[prevLogIndex].Term, and search its log index for the first index
	// where entry has term equal to conflictTerm

	if args.PrevLogIndex > rf.getLastIndex() {
		reply.ConflictIndex = rf.getLastIndex() + 1 // need + 1
		reply.Success = false
		reply.ConflictTerm = NULL

	} else if rf.log[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
		DPrintf("the server[%v] and the leader[%v] don't match in 心跳\n", rf.me, args.LeaderId)
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex].LogTerm
		conflictIndex := args.PrevLogIndex

		if conflictIndex > 0 && rf.log[conflictIndex].LogTerm == reply.ConflictTerm {
			l := 1
			r := conflictIndex
			for l < r {
				mid := (l + r) >> 1
				if rf.log[mid].LogTerm == reply.ConflictTerm {
					r = mid
				} else {
					l = mid + 1
				}

			}
			l--
			conflictIndex = l
			if conflictIndex == -1 {
				DPrintf("!!!final wrong here! for server[%d], his args's PrevLogIndex is [%d], reply's ConflictTerm is [%d]", rf.me, args.PrevLogIndex, reply.ConflictTerm)
				os.Exit(3)
			}
		}
		reply.ConflictIndex = conflictIndex + 1
		DPrintf("the server[%v]'s conflictIndex is [%v], conflictTerm is [%v], it will respond to leader[%v],"+
			"and the rf.log[reply.ConflictIndex].LogTerm is [%v]\n",
			rf.me, reply.ConflictIndex, reply.ConflictTerm, args.LeaderId, rf.log[reply.ConflictIndex].LogTerm)
	} else if rf.log[args.PrevLogIndex].LogTerm == args.PrevLogTerm {
		if rf.outOfOrderAppendEntries(args) { // 如果是过时的心跳包
			reply.Success = false
			DPrintf("发现out of order AppendEntries 来自于leader[%v], server是[%v], 它的term是[%v]", args.LeaderId,
				rf.me, rf.currentTerm)
			reply.ConflictTerm = rf.getLastTerm()
			reply.ConflictIndex = rf.getLastIndex() + 1

		} else if len(args.Entries) != 0 {
			DPrintf("得到一个心跳包，截log之前，server[%v]的log上的lastIndex是%v, lastTerm是%v它自身的currentTerm是[%v]"+
				", leader的currentTerm是[%v]\n",
				rf.me, rf.getLastIndex(), rf.getLastTerm(), rf.currentTerm, args.Term)
			rf.log = rf.log[:args.PrevLogIndex+1]
			DPrintf("得到一个心跳包，截去一段log，所以现在server[%v]的log上的lastIndex是%v, lastTerm是%v, 它自身的currentTerm是[%v]"+
				", leader的currentTerm是[%v]\n",
				rf.me, rf.getLastIndex(), rf.getLastTerm(), rf.currentTerm, args.Term)
			rf.log = append(rf.log, args.Entries...)
			DPrintf("得到一个心跳包，截去一段log后添加来自leader[%v]的一段日志后，所以现在server[%v]的log上的lastIndex是%v, lastTerm是%v, 它自身的currentTerm是[%v]"+
				", leader的currentTerm是[%v]\n",
				args.LeaderId, rf.me, rf.getLastIndex(), rf.getLastTerm(), rf.currentTerm, args.Term)
			reply.Success = true
			need_persist = true
		} else {
			// 空心跳包
			reply.Success = true
		}
	} else {
		panic("不应该到这里")
	}
	// DPrintf("for server[%d] leaderCommit is [%d], commitIndex is [%d]", rf.me, args.LeaderCommit, rf.commitIndex)
	// 如果rf.lastApplied > args.PrevLogIndex
	if reply.Success && args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastIndex())
		rf.notifyApplyCh <- struct{}{}
	}

	if need_persist {
		rf.persist()
	}

	//DPrintf("return reply success server is [%d], his reply's Term is [%d]", rf.me, reply.Term)
	rf.unlock("AppendEntries")
	return
}

// sendAppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	rf.lock("kill")
	// defer rf.unlock("kill")
	if rf.state == LEADER {
		// rf.heartbeatTimer.Stop()
		rf.stop(rf.heartbeatTimer)
	}
	close(rf.stopCh)
	atomic.StoreInt32(&rf.dead, 1)
	// rf.electionTimer.Stop()
	rf.stop(rf.electionTimer)
	rf.unlock("kill")
}

func (rf *Raft) killed() bool {
	living := atomic.LoadInt32(&rf.dead)
	return living == 1
}

func (rf *Raft) broadcastAppendEntries(term int) {

	//N := rf.commitIndex
	//last := rf.getLastIndex()
	//rf.updateLastCommit()
	// rf.lock("broadcastAppendEntries-0")
	rf.lock("broadcastAppend")
	defer rf.unlock("broadcastAppend")
	if rf.state != LEADER || rf.currentTerm != term {
		return
	}
	var argsArray = make([]AppendEntriesArgs, len(rf.peers))
	for j := range rf.peers {
		if j == rf.me {
			rf.nextIndex[j] = len(rf.log)
			rf.matchIndex[j] = len(rf.log) - 1
			continue
		}

		argsArray[j].Term = rf.currentTerm
		argsArray[j].LeaderId = rf.me
		argsArray[j].PrevLogIndex = rf.getPrevLogIndex(j)
		argsArray[j].PrevLogTerm = rf.log[rf.nextIndex[j]-1].LogTerm
		argsArray[j].LeaderCommit = rf.commitIndex
		// use deep copy to avoid race condition
		// when override log in AppendEntries()
		var entries []LogEntry
		if rf.nextIndex[j] <= rf.getLastIndex() {
			entries = make([]LogEntry, len(rf.log[rf.nextIndex[j]:]))
			copy(entries, rf.log[rf.nextIndex[j]:])
		} else {
			entries = []LogEntry{}
		}
		argsArray[j].Entries = entries
	}

	for j := range rf.peers {
		if j == rf.me {
			continue
		}
		if j != rf.me {
			go func(idx int, args *AppendEntriesArgs) {

				DPrintf("the leader[%v] begin to broadcastAppendEntries to server[%v]", rf.me, idx)
				var reply AppendEntriesReply

				ok := rf.sendAppendEntries(idx, args, &reply)
				if !ok {
					// rf.mu.Lock()
					// DPrintf("something wrong here, for server[%d], the sender is server[%d]", idx, rf.me)
					// DPrintf("server[%d]'s sendAppendEntries from [%d] in [%d] term failure, reply the term [%d]", idx, rf.me, rf.currentTerm, reply.Term)
					// rf.mu.Unlock()
					return
				}
				// 比较leader的term是否过时
				rf.lock("broadcastAppendEntries2")

				if rf.state != LEADER || rf.currentTerm != args.Term {
					rf.unlock("broadcastAppendEntries2-1")
					return
				}

				if reply.Term > rf.currentTerm && rf.state == LEADER {
					// DPrintf("Leader server[%d] outdated", rf.me)
					rf.votedFor = -1
					if rf.state == LEADER {
						// rf.heartbeatTimer.Stop()
						rf.stop(rf.heartbeatTimer)
						DPrintf("the leader[%v] converts to follower in broadcastAppendEntries, 因为收到比他[%v]高的term[%v]的心跳回复\n", rf.me,
							rf.currentTerm, reply.Term)
					}
					rf.currentTerm = reply.Term
					rf.convertTo(FOLLOWER)
					// rf.electionTimer.Stop()
					rf.stop(rf.electionTimer)
					rf.electionTimer.Reset(rf.getRandomElectionTimeOut()) // 因为之前leader停了electionTimer,所以需要恢复回来

					rf.persist()
					// DPrintf("Leader outdated cannot be ")
					rf.unlock("broadcastAppendEntries2-2")
					return
				}
				// reply.Term <= rf.currentTerm
				//DPrintf("whether [%d] come here, reply.Term is[%d], currentTerm is[%d]", rf.me, reply.Term, rf.currentTerm)
				if args.Term == rf.currentTerm && rf.state == LEADER {
					// DPrintf("server [%d] come on", idx)
					if reply.Success == false {
						// 当收到conflict回复，leader首先找到日志里的conflictTerm,找到后
						// 设置nextIndex为最后一个conflictTerm所对应的index + 1
						// 如果没有找到conflictTerm,设nextIndex为conflictIndex
						rf.nextIndex[idx] = reply.ConflictIndex

						if reply.ConflictTerm != NULL {

							for i := args.PrevLogIndex; i > 0; i-- {
								if rf.log[i].LogTerm == reply.ConflictTerm {
									DPrintf("server [%d] come on", idx)
									rf.nextIndex[idx] = i + 1
									break
								}
							}
						}

					} else {
						// 成功匹配
						rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[idx] = rf.matchIndex[idx] + 1
						// DPrintf("leader is server[%d], his commit is %d", args.LeaderId, args.LeaderCommit)
						if len(args.Entries) > 0 && rf.state == LEADER {
							rf.updateLastCommit()
						}
					}
				}
				rf.unlock("broadcastAppendEntries2-3")
			}(j, &argsArray[j])
		}

	}
}

func (rf *Raft) broadcastRequestVote(term int) {
	rf.lock("broadcastRequest")

	defer rf.unlock("broadcastRequest")
	if rf.state != CANDIDATE || rf.currentTerm != term {
		return
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	// 候选人获得的票数
	var voteAcquired int32 = 0

	//rf.currentTerm += 1

	for i := range rf.peers {
		if i == rf.me {
			// rf.votedFor = rf.me
			atomic.AddInt32(&voteAcquired, 1)

			DPrintf("候选人[%v]给自己投票\n", rf.me)
			continue
		}

		go func(idx int) {

			//	rf.mu.Unlock()
			//	return
			//}
			//rf.mu.Unlock()
			// reply := &RequestVoteReply{}
			var reply RequestVoteReply
			// rf.lock("broadcastRequestVote-0")
			// if rf.state != CANDIDATE || rf.currentTerm != args.Term {
			//
			//	rf.unlock("broadcastRequestVote-0_1")
			//	return
			// }

			// rf.unlock("broadcastRequestVote-0_2")
			ok := rf.sendRequestVote(idx, &args, &reply)

			if ok {
				rf.lock("broadcastRequestVote")

				if rf.state != CANDIDATE || rf.currentTerm != args.Term {
					DPrintf("候选人[%v]状态改变了，目前它是%v, term是%v, 收到的reply的term是[%v]\n", rf.me,
						rf.state, rf.currentTerm, reply.Term)
					rf.unlock("broadcastRequestVote-1")

					return
				}

				if reply.VoteGranted && rf.state == CANDIDATE && args.Term == rf.currentTerm {
					atomic.AddInt32(&voteAcquired, 1)
					DPrintf("收到来自server[%v]的投票给候选人[%v]\n", idx, rf.me)
					if atomic.LoadInt32(&voteAcquired) > int32(len(rf.peers)/2) {
						// rf.electionTimer.Stop()
						rf.stop(rf.electionTimer)
						rf.convertTo(LEADER)

						DPrintf("收到超过半数的投票，候选人[%v]即将转化为leader\n", rf.me)
						for i := range rf.nextIndex {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}
						for i := range rf.matchIndex {
							rf.matchIndex[i] = 0
						}
						rf.matchIndex[rf.me] = rf.getLastIndex()

						rf.votedFor = -1
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						if rf.state == LEADER {
							// rf.heartbeatTimer.Stop()
							rf.stop(rf.heartbeatTimer)
							DPrintf("the leader[%v] convert to Follower in broadcastRequestVote\n", rf.me)
						}

						rf.convertTo(FOLLOWER)
						// rf.electionTimer.Stop()
						rf.stop(rf.electionTimer)
						rf.electionTimer.Reset(rf.getRandomElectionTimeOut())

						rf.persist()
						// Here we need to set the candidate's vote to null

					}

				}
				rf.unlock("broadcastRequestVote-4")
			}
		}(i)

	}
}

// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine
func (rf *Raft) updateLastApplied() {
	rf.lock("UpdateLastApplied")
	defer rf.unlock("UpdateLastApplied")
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		command := rf.log[rf.lastApplied].Command
		applyMsg := ApplyMsg{true, command, rf.lastApplied}
		// DPrintf("updateLastApplied, server[%d]", rf.me)
		rf.applyCh <- applyMsg
		// DPrintf("final apply from server[%d], his state is %v", rf.me, rf.state)
	}

}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
// make the raft Leader's replcation and commit decoupling
func (rf *Raft) updateLastCommit() {
	// rf.lock("updateLastCommit")
	// defer rf.unlock("updateLastCommit")
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	// for i := range rf.matchIndex {
	//	DPrintf("matchIndex[%d] is %d", i, rf.matchIndex[i])
	// }

	// sort.Sort(sort.IntSlice(matchIndexCopy))
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndexCopy)))
	N := matchIndexCopy[len(matchIndexCopy)/2]
	// for i := range rf.log {
	//	DPrintf("server[%d] %v", rf.me, rf.log[i])
	// }
	// for i := range rf.matchIndex {
	// 	DPrintf("server[%d]'s matchindex is %v", i, rf.matchIndex[i])
	// }
	// Check
	N = Min(N, rf.getLastIndex())

	if N > rf.commitIndex && rf.log[N].LogTerm == rf.currentTerm && rf.state == LEADER {
		rf.commitIndex = N
		// DPrintf("updateLastCommit from server[%d]", rf.me)
		rf.notifyApplyCh <- struct{}{}

	}

}

// 这一部分只处理timer的设置和raft的state，把Raft其他状态的设置放到对应的位置处理
// 这一部分的加锁处理放在外层调用
func (rf *Raft) convertTo(nodestate State) {
	//if nodestate == rf.state {
	//	return
	//}
	//if nodestate == CANDIDATE {
	//	rf.currentTerm += 1
	//}
	// DPrintf("Term %d: server %d convert from %v to %v\n",
	//	rf.currentTerm, rf.me, rf.state, nodestate)
	rf.state = nodestate
	switch nodestate {
	case FOLLOWER:
		DPrintf("the server[%v] become the follower, its term is %v, its last index is %v, last index's term is %v\n",
			rf.me, rf.currentTerm, rf.getLastIndex(), rf.getLastTerm())
	case CANDIDATE:
		rf.votedFor = rf.me
		rf.currentTerm += 1
		DPrintf("the server[%v] become the candidate, its term is %v, its last index is %v, last index's term is %v\n",
			rf.me, rf.currentTerm, rf.getLastIndex(), rf.getLastTerm())
		// rf.electionTimer.Stop()
		rf.stop(rf.electionTimer)
		DPrintf("what happen\n")
		rf.electionTimer.Reset(rf.getRandomElectionTimeOut())
		DPrintf("now\n")
		DPrintf("the server[%v] converted to candidate in convertTo function at the time %v\n",
			rf.me, time.Now())
		rf.persist()
		go rf.broadcastRequestVote(rf.currentTerm)

	case LEADER:
		DPrintf("the server[%v] become the leader, its term is %v, its last index is %v, last index's term is %v\n",
			rf.me, rf.currentTerm, rf.getLastIndex(), rf.getLastTerm())
		rf.heartbeatTimer.Reset(HeartbeatInterval)
		go rf.broadcastAppendEntries(rf.currentTerm)

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
	rf.log = make([]LogEntry, 1) //[]LogEntry{{0, nil}} // first index is 1, first term  is also 1 , {LogTerm, Command}
	rf.currentTerm = 0

	//rf.leaderCh = make(chan interface{})
	//rf.commitCh = make(chan interface{})

	//rf.electionTimerResetChan = make(chan bool)
	//rf.heartbeatResetChan = make(chan bool)
	// nextIndex和matchIndex只有leader用
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastIndex() + 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh

	rf.lastApplied = 0
	rf.commitIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Modify Make() to create a background goroutine that will kick off leader election
	// periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	// This way a peer will learn who is the leader, if there is already a leader, or become the leader itself.

	rf.electionTimer = time.NewTimer(rf.getRandomElectionTimeOut())
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.stopCh = make(chan struct{})
	rf.notifyApplyCh = make(chan struct{})
	go func() {

		for !rf.killed() {

			select {
			case <-rf.electionTimer.C:
				rf.lock("electionTimer")

				if rf.state == LEADER {
					rf.unlock("electionTimer1")
					break
				}

				if rf.state == FOLLOWER || rf.state == CANDIDATE {
					//DPrintf("ElectionTimer time out")
					rf.convertTo(CANDIDATE)
					// when the raft server becomes candidate
					// we should put the currentTerm update
					// and votedFor update in broadcastAppendEntries
					// or we have not time to win in the split vote
					// situation


				}
				rf.unlock("electionTimer")

			case <-rf.heartbeatTimer.C:
				rf.lock("heartbeatTimer")

				if rf.state == LEADER {
					// rf.heartbeatTimer.Stop()
					rf.stop(rf.heartbeatTimer)
					rf.heartbeatTimer.Reset(HeartbeatInterval)
					go rf.broadcastAppendEntries(rf.currentTerm)
				}
				rf.unlock("heartbeatTimer")

			case <-rf.stopCh:
				return
			}

		}
	}()

	go func() {
		for !rf.killed() {
			select {
			case <-rf.notifyApplyCh:
				go rf.updateLastApplied()
			}
		}
	}()

	return rf
}
