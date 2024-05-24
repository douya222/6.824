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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// import "bytes"
// import "labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Follower           int = 1
	Candidate          int = 2
	Leader             int = 3
	HEART_BEAT_TIMEOUT     = 100 //心跳超时，要求1秒10次，所以是100ms一次
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTimer  *time.Timer   // 选举定时器
	heartbeatTimer *time.Timer   // 心跳定时器
	state          int           // 角色
	voteCount      int           //投票数
	applyCh        chan ApplyMsg // 提交通道

	currentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        //candidateId that received vote in current term (or null if none)
	log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	//Volatile state on leaders:(Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// 2D
	snapshot SnapShot
}

// 2D
type SnapShot struct {
	lastIncludedTerm  int
	lastIncludedIndex int
	data              []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm) // 图2说明的要持久化的数据
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// 2D 持久化
	e.Encode(rf.snapshot.lastIncludedTerm)
	e.Encode(rf.snapshot.lastIncludedIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshot SnapShot // 2D 持久化
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&snapshot.lastIncludedTerm) != nil || d.Decode(&snapshot.lastIncludedIndex) != nil {
		panic("fail to decode data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		// 2D 持久化
		rf.snapshot = snapshot
		rf.lastApplied = snapshot.lastIncludedIndex - 1
		rf.commitIndex = snapshot.lastIncludedIndex - 1
	}
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
	Term int // currentTerm, for leader to update itself
}

// InstallSnapshot RPC，发送端的处理逻辑
// 快照同步，执行时机为 Leader 发现 AppendEntries 中的 PrevIndex < snapshot.lastIncludedIndex，则表示需要同步的日志在快照中，此时需要同步整个快照，并且修改 nextIndex 为 snapshot.lastIncludedIndex。
func (rf *Raft) SendInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.snapshot.lastIncludedIndex,
		LastIncludedTerm:  rf.snapshot.lastIncludedTerm,
		// hint: Send the entire snapshot in a single InstallSnapshot RPC.
		// Don't implement Figure 13's offset mechanism for splitting up the snapshot.
		Data: rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if ok {
		// check reply term
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Term > args.Term {
			// DPrintf("[SendInstallSnapshot] %v to %d failed because reply.Term > args.Term, reply=%v\n", rf.role_info(), server, reply)
			rf.switchStateTo(Follower)
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}

		// update nextIndex and matchIndex
		rf.nextIndex[server] = args.LastIncludedIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		// DPrintf("[SendInstallSnapshot] %s to %d nextIndex=%v, matchIndex=%v", rf.role_info(), server, rf.nextIndex, rf.matchIndex)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// InstallSnapshot RPC，客户端的处理逻辑
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 1. Reply immediately if term < currentTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.snapshot.lastIncludedIndex {
		// DPrintf("[InstallSnapshot] %s reject Install Snapshot args=%v, rf.lastIncludeIndex=%d", rf.role_info(), args, rf.snapshot.lastIncludedIndex)
		return
	}
	// DPrintf("[InstallSnapshot] %s recive InstallSnapshot rpc %v", rf.role_info(), args)
	defer rf.persist()
	if rf.currentTerm < args.Term {
		rf.switchStateTo(Follower)
		rf.currentTerm = args.Term
	}
	rf.electionTimer.Reset(randTimeDuration())

	rf.snapshot.data = args.Data
	rf.commitIndex = args.LastIncludedIndex - 1
	rf.lastApplied = args.LastIncludedIndex - 1
	realIndex := rf.logicIndexToRealIndex(args.LastIncludedIndex) - 1
	// DPrintf("[InstallSnapshot] %s commitIndex=%d, Log=%v", rf.role_info(), rf.commitIndex, rf.log)
	if rf.getLogLogicSize() <= args.LastIncludedIndex {
		rf.log = []LogEntry{}
	} else {
		rf.log = append([]LogEntry{}, rf.log[realIndex+1:]...)
	}
	rf.snapshot.lastIncludedIndex = args.LastIncludedIndex
	rf.snapshot.lastIncludedTerm = args.LastIncludedTerm
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), rf.snapshot.data)
	}()
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshot.lastIncludedIndex {
		return
	}
	defer rf.persist()
	// get real index
	realIndex := rf.logicIndexToRealIndex(index) - 1
	// save snapshot
	rf.snapshot.data = snapshot //append(rf.snapshot.data, snapshot...)
	rf.snapshot.lastIncludedTerm = rf.log[realIndex].Term
	// discard before index log
	if rf.getLogLogicSize() <= index {
		rf.log = []LogEntry{}
	} else {
		rf.log = append([]LogEntry{}, rf.log[realIndex+1:]...)
	}
	rf.snapshot.lastIncludedIndex = index
	rf.lastApplied = rf.snapshot.lastIncludedIndex - 1
	// DPrintf("[Snapshot] %s do snapshot, index = %d, lastApplied=%d, rf.log=%v", rf.role_info(), index, rf.lastApplied, rf.log)
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), rf.snapshot.data)

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("Candidate[raft%v][term:%v] request vote: raft%v[%v] 's term%v\n", args.CandidateId, args.Term, rf.me, rf.state, rf.currentTerm)
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}

	// 2B: candidate's vote should be at least up-to-date as receiver's log
	// "up-to-date" is defined in thesis 5.4.1
	lastLogIndex := rf.getLastLogLogicIndex()
	lastLogTerm := rf.snapshot.lastIncludedTerm
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[rf.getLastLogRealIndex()].Term
	}
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm &&
			args.LastLogIndex < (lastLogIndex)) {
		// Receiver is more up-to-date, does not grant vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	// reset timer after grant vote
	rf.electionTimer.Reset(randTimeDuration())
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		rf.persist() // 2C 持久化
		index = len(rf.log) - 1
		rf.matchIndex[rf.me] = index // 只修改了自己的
		rf.nextIndex[rf.me] = index + 1
		return rf.getLogLogicSize(), term, isLeader
	}

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

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.currentTerm = 1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = Follower
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(HEART_BEAT_TIMEOUT * time.Millisecond)
	rf.electionTimer = time.NewTimer(randTimeDuration())

	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = rf.getLogLogicSize()
	}

	rf.mu.Unlock()

	rf.readPersist(persister.ReadRaftState())
	//以定时器的维度重写background逻辑
	go func() {
		for !rf.killed() {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				switch rf.state {
				case Follower:
					rf.switchStateTo(Candidate)
				case Candidate:
					rf.startElection()
				}
				rf.mu.Unlock()

			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == Leader {
					rf.heartbeats()
					rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}

func randTimeDuration() time.Duration {
	return time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
}

// 切换状态，调用者需要加锁
func (rf *Raft) switchStateTo(state int) {
	if state == rf.state {
		return
	}
	DPrintf("Term %d: server %d convert from %v to %v\n", rf.currentTerm, rf.me, rf.state, state)
	rf.state = state
	switch state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration())
		rf.votedFor = -1

	case Candidate:
		//成为候选人后立马进行选举
		rf.startElection()

	case Leader:
		// initialized to leader last log index + 1
		for i := range rf.peers {
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = rf.getLogLogicSize() // 由于数组下标从 0 开始
		}

		rf.electionTimer.Stop()
		rf.heartbeats()
		rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
	}
}

// 发送心跳包，调用者需要加锁
func (rf *Raft) heartbeats() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.heartbeat(i)
		}
	}
}

func (rf *Raft) heartbeat(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[server] - 1
	// use deep copy to avoid race condition
	// when override log in AppendEntries()
	// entries := make([]LogEntry, len(rf.log[(prevLogIndex+1):]))
	// copy(entries, rf.log[(prevLogIndex+1):])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		LeaderCommit: rf.commitIndex,
	}
	realPrevLogIndex := rf.logicIndexToRealIndex(prevLogIndex)
	if realPrevLogIndex >= 0 && rf.getLogRealSize() != 0 {
		args.PrevLogTerm = rf.log[realPrevLogIndex].Term
	}
	// 2D
	// 如果有日志还没被 commit
	if rf.getLastLogLogicIndex() != rf.matchIndex[server] {
		// hint: have the leader send an InstallSnapshot RPC if it doesn't have the log entries required to bring a follower up to date.
		if rf.nextIndex[server] < rf.snapshot.lastIncludedIndex {
			rf.mu.Unlock()
			go rf.SendInstallSnapshot(server)
			return
		} else {
			startIdx := rf.logicIndexToRealIndex(rf.nextIndex[server])
			if startIdx < rf.getLogRealSize() {
				args.Entries = make([]LogEntry, len(rf.log)-startIdx)
				copy(args.Entries, rf.log[startIdx:])
			}
		}
	}

	rf.mu.Unlock()

	var reply AppendEntriesReply
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		// • If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		// • If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		if reply.Success {
			// successfully replicated args.Entries
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).
			for N := rf.getLastLogRealIndex(); N > rf.logicIndexToRealIndex(rf.commitIndex); N-- {
				count := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= N {
						count += 1
					}
				}

				if count > len(rf.peers)/2 {
					// most of nodes agreed on rf.log[i]
					rf.setCommitIndex(rf.realIndexToLogicIndex(N))
					break
				}
			}

		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.switchStateTo(Follower)
				rf.persist() // 2C 持久化
				return
			} else {
				//如果走到这个分支，那一定是需要前推，修改next Index，等待下一轮RPC执行，因为直接retry并没有明显优势
				rf.nextIndex[server] = reply.ConflictIndex
				// if term found, override it to
				// the first entry after entries in ConflictTerm
				if reply.ConflictTerm != -1 {
					for i := rf.logicIndexToRealIndex(args.PrevLogIndex) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							// in next trial, check if log entries in ConflictTerm matches
							rf.nextIndex[server] = rf.realIndexToLogicIndex(i)
							break
						}
					}
				}
			}
		}
	}
}

// 开始选举，调用者需要加锁
func (rf *Raft) startElection() {

	//    DPrintf("raft%v is starting election\n", rf.me)
	rf.currentTerm += 1
	rf.votedFor = rf.me //vote for me
	rf.persist()        // 2C 持久化
	rf.voteCount = 1
	rf.electionTimer.Reset(randTimeDuration())

	for i := range rf.peers {
		if i != rf.me {
			// 由于 sendRpc 会阻塞，所以这里选择新启动 goroutine 去 sendRPC，不阻塞当前协程
			go func(peer int) {
				rf.mu.Lock()
				// lastLogIndex := len(rf.log) - 1
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastLogLogicIndex(),
					LastLogTerm:  rf.snapshot.lastIncludedTerm,
				}
				if len(rf.log) != 0 {
					args.LastLogTerm = rf.log[rf.getLastLogRealIndex()].Term
				}
				// DPrintf("raft%v[%v] is sending RequestVote RPC to raft%v\n", rf.me, rf.state, peer)
				rf.mu.Unlock()
				var reply RequestVoteReply
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.switchStateTo(Follower)
						rf.persist() // 2C
						return
					}
					if reply.VoteGranted && rf.state == Candidate {
						rf.voteCount++
						if rf.voteCount > len(rf.peers)/2 {
							rf.switchStateTo(Leader)
						}
					}
				}
			}(i)
		}
	}
}

/*
1. heart beat
2. log replication
*/

func (rf *Raft) getLastLogLogicIndex() int {
	return len(rf.log) - 1 + rf.snapshot.lastIncludedIndex
}

func (rf *Raft) getLastLogRealIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLogLogicSize() int {
	return len(rf.log) + rf.snapshot.lastIncludedIndex
}

func (rf *Raft) getLogRealSize() int {
	return len(rf.log)
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	if index < 0 || index >= len(rf.log) {
		return LogEntry{}
	} else {
		return rf.log[index]
	}
}

func (rf *Raft) sliceLog(startIdx int, endIdx int) {
	if len(rf.log) == 0 || endIdx < 0 {
		return
	}

	if startIdx < 0 {
		startIdx = 0
	}

	if endIdx > len(rf.log) {
		endIdx = len(rf.log)
	}

	rf.log = rf.log[startIdx:endIdx]
}

func (rf *Raft) logicIndexToRealIndex(logicIndex int) int {
	return logicIndex - rf.snapshot.lastIncludedIndex
}

func (rf *Raft) realIndexToLogicIndex(realIndex int) int {
	return realIndex + rf.snapshot.lastIncludedIndex
}

type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

	ConflictTerm  int // 2C
	ConflictIndex int // 2C

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // 2C 持久化
	DPrintf("leader[raft%v][term:%v] beat term:%v [raft%v][%v]\n", args.LeaderId, args.Term, rf.currentTerm, rf.me, rf.state)
	reply.Success = true

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}

	// reset election timer even log does not match
	// args.LeaderId is the current term's Leader
	rf.electionTimer.Reset(randTimeDuration())

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// lastLogIndex := len(rf.log) - 1
	lastLogIndex := rf.getLastLogLogicIndex()
	// 如果preLogIndex的长度大于当前的日志的长度，那么说明跟随者缺失日志
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		// optimistically thinks receiver's log matches with Leader's as a subset
		// reply.ConflictIndex = len(rf.log) // 2C 优化
		reply.ConflictIndex = lastLogIndex + 1
		// no conflict term
		reply.ConflictTerm = -1 // 2C 优化
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 如果preLogIndex处的任期和preLogTerm不相等，那么说明日志有冲突
	realPrevLogIndex := rf.logicIndexToRealIndex(args.PrevLogIndex)
	if realPrevLogIndex >= 0 && rf.log[realPrevLogIndex].Term != args.PrevLogTerm {
		realPrevLogTerm := rf.log[realPrevLogIndex].Term
		reply.Success = false
		reply.Term = rf.currentTerm
		// 一次跳过一个Term，而非一个Index
		// expecting Leader to check the former term
		// so set ConflictIndex to the first one of entries in ConflictTerm
		reply.ConflictTerm = realPrevLogTerm
		conflictIndex := realPrevLogIndex
		// 搞清楚rf.log[0]的意义
		// apparently, since rf.log[0] are ensured to match among all servers
		// ConflictIndex must be > 0, safe to minus 1
		for conflictIndex-1 >= 0 && rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = rf.realIndexToLogicIndex(conflictIndex)
		return
	}

	// 4. Append any new entries not already in the log
	// compare from rf.log[args.PrevLogIndex + 1]
	// 逐一比对要附加的日志entries[]是否和自己preLogIndex之后的日志是否有冲突
	// 1. 如果entries[]中的任何一位置的日志发生冲突，那么需要将之后的日志进行截断，并追加为entries[]中之后的日志
	// 2. 如果没有发生冲突，那么存在两种情况：
	//		- 跟随者的日志和entries[]的日志全部匹配，这种情况可能是重复的附加日志RPC，那么这种情况只会简单的校验一遍所有日志
	//		- entries[]的日志匹配当前的所有日志，那么将没有匹配的日志全都追加到当前的日志后面。
	// 如果不对现有的日志进行比对，而且简单的进行截断追加日志，那么是很危险的
	// 因为可能收到延时的重复日志附加请求而导致日志不必要的截断，从而导致已经提交的日志丢失
	unmatch_idx := -1
	for idx := range args.Entries {
		index := args.PrevLogIndex + 1 + idx
		entry := rf.getLogEntry(rf.logicIndexToRealIndex(index))
		if rf.getLogLogicSize() < index+1 || entry.Term != args.Entries[idx].Term {
			// unmatch log found
			unmatch_idx = idx
			break
		}
	}

	if unmatch_idx != -1 {
		// there are unmatch entries
		// truncate unmatch Follower entries, and apply Leader entries
		// 防止race ,因为切片还是引用,深拷贝
		append_entries := make([]LogEntry, len(args.Entries)-unmatch_idx)
		copy(append_entries, args.Entries[unmatch_idx:])
		rf.sliceLog(0, rf.logicIndexToRealIndex(args.PrevLogIndex)+1+unmatch_idx)
		// rf.log = rf.log[:(args.PrevLogIndex + 1 + unmatch_idx)]
		rf.log = append(rf.log, append_entries...)
	}

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(min(args.LeaderCommit, rf.getLastLogLogicIndex()))
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// several setters, should be called with a lock
func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	// apply all entries between lastApplied and committed
	// should be called after commitIndex updated
	if rf.commitIndex > rf.lastApplied {
		commitIndex = rf.logicIndexToRealIndex(commitIndex)
		lastApplied := rf.logicIndexToRealIndex(rf.lastApplied)

		// DPrintf("%v apply from index %d to %d", rf, rf.lastApplied+1, rf.commitIndex)
		entriesToApply := append([]LogEntry{}, rf.log[lastApplied+1:commitIndex+1]...)

		go func(startIdx int, entries []LogEntry) {
			for idx, entry := range entries {
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = startIdx + idx + 1 // command index require start at 1
				rf.applyCh <- msg
				// do not forget to update lastApplied index
				// this is another goroutine, so protect it with lock
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex-1 {
					rf.lastApplied = msg.CommandIndex - 1
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}
}

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}
