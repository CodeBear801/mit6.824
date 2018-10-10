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
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

type ServerState string // server state

const (
	Follower  ServerState = "Follower"
	Candidate             = "Candidate"
	Leader                = "Leader"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
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
	UseSnapshot  bool
	Snapshot     []byte
}

type RaftPersistence struct {
	CurrentTerm       int
	Log               []LogEntry
	VotedFor          string
	LastSnapshotIndex int
	LastSnapshotTerm  int
	// LastApplied       int
	// CommitIndex       int
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

	log         []LogEntry
	commitIndex int
	lastApplied int

	lastSnapshotIndex int
	lastSnapshotTerm  int

	id string // server's name

	isDecommissioned bool        // is raft server valid or not
	state            ServerState // current states

	currentTerm int
	voteForID   string
	leaderID    string

	nextIndex      []int
	matchIndex     []int
	sendAppendChan []chan struct{}

	lastHeartBeat time.Time
}

func (rf *Raft) PrintInfo() {
	RaftInfo("*** print info", rf)
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	// Your code here (2A).
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
	buf := new(bytes.Buffer)
	raftPersistenceObj := RaftPersistence{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.voteForID,
		LastSnapshotIndex: rf.lastSnapshotIndex,
		LastSnapshotTerm:  rf.lastSnapshotTerm,
		// LastApplied:       rf.lastApplied,
		// CommitIndex:       rf.commitIndex,
	}
	raftPersistenceObj.Log = make([]LogEntry, len(rf.log))
	copy(raftPersistenceObj.Log, rf.log)

	gob.NewEncoder(buf).Encode(raftPersistenceObj)
	rf.persister.SaveRaftState(buf.Bytes())
	//RaftInfo("Saved persisted node data (%d bytes). log size is %d  }", rf, len(buf.Bytes()), len(raftPersistenceObj.Log))

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

	buf := bytes.NewBuffer(data)
	tempgob := gob.NewDecoder(buf)
	tempobj := RaftPersistence{}
	tempgob.Decode(&tempobj)

	//RaftInfo("Debug tempobj lastapplied = %d, commitIndex = %d, len(log) = %d", rf, tempobj.LastApplied, tempobj.CommitIndex, len(tempobj.Log))

	rf.currentTerm = tempobj.CurrentTerm
	rf.log = tempobj.Log
	rf.voteForID = tempobj.VotedFor
	rf.lastSnapshotIndex = tempobj.LastSnapshotIndex
	rf.lastSnapshotTerm = tempobj.LastSnapshotTerm
	// rf.lastApplied = tempobj.LastApplied
	// rf.commitIndex = tempobj.CommitIndex

	//RaftInfo("Loaded persisted node data (%d bytes). Last applied index: %d, log size is %d }", rf, len(data), rf.lastApplied, len(rf.log))
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Id          string
}

func (reply *RequestVoteReply) VoteCount() int {
	if reply.VoteGranted {
		return 1
	} else {
		return 0
	}
}

type AppendEntiesArgs struct {
	Term             int
	LeaderId         string
	PreviousLogIndex int
	PreviousLogTerm  int
	LogEntries       []LogEntry
	LeaderCommit     int
}

type AppendEntriesReply struct {
	Term                int
	Success             bool
	ConflictingLogTerm  int
	ConflictingLogIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	defer rf.Unlock()
	RaftInfo("Get Request from %s", rf, args.CandidateId)
	lastIndex, lastTerm := rf.getLastEntryInfo()
	isLogUpToDate := func() bool {
		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LastLogIndex
		} else {
			return lastTerm < args.LastLogTerm
		}
	}()

	reply.Term = rf.currentTerm
	reply.Id = rf.id

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term >= rf.currentTerm && isLogUpToDate {
		rf.transitionToFollower(args.Term)
		rf.voteForID = args.CandidateId
		reply.VoteGranted = true
	} else if (rf.voteForID == "" || args.CandidateId == rf.voteForID) && isLogUpToDate {
		rf.voteForID = args.CandidateId
		reply.VoteGranted = true
	}

	rf.persist()
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
func (rf *Raft) sendRequestVote(serverConn *labrpc.ClientEnd, server int, voteChan chan int, args *RequestVoteArgs, reply *RequestVoteReply) {

	request := func() bool {
		return serverConn.Call("Raft.RequestVote", args, reply)
	}

	if ok := SendRPCRequest("Raft.RequestVote", request); ok {
		voteChan <- server
	}

	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()

	if !isLeader {
		return -1, term, isLeader
	}

	rf.Lock()
	defer rf.Unlock()
	nextIndex := func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Index + 1
		}
		return Max(1, rf.lastSnapshotIndex+1)
	}()

	entry := LogEntry{Index: nextIndex, Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	//RaftInfo("New entry appended to leader's log: %s", rf, entry)

	return nextIndex, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.Lock()
	defer rf.Unlock()

	rf.isDecommissioned = true
}

func (rf *Raft) startElectionProcess() {
	electionTimeout := func() time.Duration {
		return (200 + time.Duration(rand.Intn(150))) * time.Millisecond
	}

	currentTimeout := electionTimeout()
	currentTime := <-time.After(currentTimeout)

	rf.Lock()
	defer rf.Unlock()

	if !rf.isDecommissioned {
		if rf.state != Leader && currentTime.Sub(rf.lastHeartBeat) >= currentTimeout {
			RaftInfo("Election timer timed out. Timeout: %fs", rf, currentTimeout.Seconds())
			go rf.beginElection()
		}
		go rf.startElectionProcess()
	}

}

func (rf *Raft) transitionToCandidate() {
	//rf.Lock()
	//defer rf.Unlock()

	rf.state = Candidate
	rf.currentTerm++
	rf.voteForID = rf.id
}

func (rf *Raft) transitionToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.voteForID = ""
}

func (rf *Raft) promoteToLeader() {
	rf.Lock()
	defer rf.Unlock()

	rf.state = Leader
	rf.leaderID = rf.id

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.sendAppendChan = make([]chan struct{}, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log) + 1
			rf.matchIndex[i] = 0
			rf.sendAppendChan[i] = make(chan struct{}, 1)

			go rf.startLeaderPeerProcess(i, rf.sendAppendChan[i])
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	defer rf.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term >= rf.currentTerm {
		rf.transitionToFollower(args.Term)
		rf.leaderID = args.LeaderId
	}

	if rf.leaderID == args.LeaderId {
		rf.lastHeartBeat = time.Now()
	}

	rf.persister.SaveSnapshot(args.Data) // Save snapshot, discarding any existing snapshot with smaller index

	i, isPresent := rf.findLogIndex(args.LastIncludedIndex)
	if isPresent && rf.log[i].Term == args.LastIncludedTerm {
		// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it
		rf.log = rf.log[i+1:]
	} else { // Otherwise discard the entire log
		rf.log = make([]LogEntry, 0)
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.lastApplied = 0 // LocalApplyProcess will pick this change up and send snapshot

	rf.persist()

	RaftInfo("Installed snapshot from %s, LastSnapshotEntry(Index: %d, Term: %d)", rf, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)

}

func (rf *Raft) sendSnapshot(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()

	peer := rf.peers[peerIndex]
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.leaderID,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.Unlock()

	// Send RPC (with timeouts + retries)
	requestName := "Raft.InstallSnapshot"
	request := func() bool {
		return peer.Call(requestName, &args, &reply)
	}
	ok := SendRPCRequest(requestName, request)

	rf.Lock()
	defer rf.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.transitionToFollower(reply.Term)
		} else {
			rf.nextIndex[peerIndex] = args.LastIncludedIndex + 1
		}
	}

	sendAppendChan <- struct{}{} // Signal to leader-peer process that there may be appends to send
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int, maxstate int) {

	rf.Lock()
	defer rf.Unlock()

	tempInitialSize := rf.GetPersistSize()

	if tempInitialSize < maxstate {
		// already be snapshot
		return
	}

	baseIndex := rf.log[0].Index
	lastIndex, _ := rf.getLastEntryInfo()

	RaftInfo("++++++ baseindex %d lastIndex %d index %d ", rf, baseIndex, lastIndex, index)
	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	var newLogEntries []LogEntry

	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: rf.log[index-baseIndex].Term, Command: rf.log[index-baseIndex].Command})

	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i-baseIndex])
	}

	rf.lastSnapshotIndex = index - 1
	rf.lastSnapshotTerm = rf.log[index-baseIndex-1].Term

	rf.log = newLogEntries

	rf.persist()

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].Index)
	e.Encode(newLogEntries[0].Term)

	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)

	tempAfterSanpshotSize := rf.GetPersistSize()
	RaftInfo("++++++ tempInitialSize = %d, tempAfterSanpshotSize = %d len(newLogEntries) = %d", rf, tempInitialSize, tempAfterSanpshotSize, len(newLogEntries))
}

func (rf *Raft) AppendEntries(args *AppendEntiesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	RaftInfo("Request from %s, w/ %d entries. Args.Prev:[Index %d, Term %d]", rf, args.LeaderId, len(args.LogEntries), args.PreviousLogIndex, args.PreviousLogTerm)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm {
		rf.transitionToFollower(args.Term)
		rf.leaderID = args.LeaderId
	}

	if rf.leaderID == args.LeaderId {
		rf.lastHeartBeat = time.Now()
	}

	// Try to find supplied previous log entry match in our log
	prevLogIndex := -1
	for i, v := range rf.log {
		if v.Index == args.PreviousLogIndex {
			if v.Term == args.PreviousLogTerm {
				prevLogIndex = i
				break
			} else {
				reply.ConflictingLogTerm = v.Term
			}
		}
	}

	PrevIsInSnapshot := args.PreviousLogIndex == rf.lastSnapshotIndex && args.PreviousLogTerm == rf.lastSnapshotTerm
	PrevIsBeginningOfLog := args.PreviousLogIndex == 0 && args.PreviousLogTerm == 0

	if prevLogIndex >= 0 || PrevIsInSnapshot || PrevIsBeginningOfLog {
		if len(args.LogEntries) > 0 {
			RaftInfo("Appending %d entries from %s", rf, len(args.LogEntries), args.LeaderId)
		}

		// Remove any inconsistent logs and find the index of the last consistent entry from the leader
		entriesIndex := 0
		for i := prevLogIndex + 1; i < len(rf.log); i++ {
			entryConsistent := func() bool {
				localEntry, leadersEntry := rf.log[i], args.LogEntries[entriesIndex]
				return localEntry.Index == leadersEntry.Index && localEntry.Term == leadersEntry.Term
			}
			if entriesIndex >= len(args.LogEntries) || !entryConsistent() {
				// Additional entries must be inconsistent, so let's delete them from our local log
				rf.log = rf.log[:i]
				break
			} else {
				entriesIndex++
			}
		}

		// Append all entries that are not already in our log
		//RaftInfo("+++ A, %d, %d", rf, entriesIndex, len(args.LogEntries))
		if entriesIndex < len(args.LogEntries) {
			//RaftInfo("+++ B", rf)
			rf.log = append(rf.log, args.LogEntries[entriesIndex:]...)
		}

		// Update the commit index
		if args.LeaderCommit > rf.commitIndex {
			var latestLogIndex = rf.lastSnapshotIndex
			if len(rf.log) > 0 {
				latestLogIndex = rf.log[len(rf.log)-1].Index
				RaftInfo("+++ rf.log[len(rf.log)-1].Index = %d", rf, latestLogIndex)
			}

			if args.LeaderCommit < latestLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = latestLogIndex
			}
		}
		RaftInfo("+++ after appending logs, current commit index is %d, len(log) is %d }", rf, rf.commitIndex, len(rf.log))
		reply.Success = true
	} else {
		// §5.3: When rejecting an AppendEntries request, the follower can include the term of the
		//	 	 conflicting entry and the first index it stores for that term.

		// If there's no entry with `args.PreviousLogIndex` in our log. Set conflicting term to that of last log entry
		if reply.ConflictingLogTerm == 0 && len(rf.log) > 0 {
			reply.ConflictingLogTerm = rf.log[len(rf.log)-1].Term
		}

		for _, v := range rf.log { // Find first log index for the conflicting term
			if v.Term == reply.ConflictingLogTerm {
				reply.ConflictingLogIndex = v.Index
				break
			}
		}

		reply.Success = false
	}

	rf.persist()
}

func (rf *Raft) sendAppendEntryRequest(server int, args *AppendEntiesArgs, reply *AppendEntriesReply) bool {
	request := func() bool {
		return rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	return SendRPCRequest("Raft.AppendEntries", request)
}

func (rf *Raft) sendAppendEntries(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()

	if rf.state != Leader || rf.isDecommissioned {
		rf.Unlock()
		return
	}

	var entries []LogEntry = []LogEntry{}
	var prevLogIndex, prevLogTerm int = 0, 0

	peerId := string(rune(peerIndex + 'A'))
	lastLogIndex, _ := rf.getLastEntryInfo()

	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[peerIndex] {
		if rf.nextIndex[peerIndex] <= rf.lastSnapshotIndex {
			rf.Unlock()
			RaftDebug("$$$ come into sendSnapshot", rf)
			rf.sendSnapshot(peerIndex, sendAppendChan)
			return
		} else {
			for i, v := range rf.log {
				if v.Index == rf.nextIndex[peerIndex] {
					if i > 0 {
						lastEntry := rf.log[i-1]
						prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
					} else {
						prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
					}
					RaftDebug("$$$ v = %v, rf.nextIndex[peerIndex] = %d (%d, %d))", rf, v, rf.nextIndex[peerIndex], prevLogIndex, prevLogTerm)
					entries = make([]LogEntry, len(rf.log)-i)
					copy(entries, rf.log[i:])
					break
				}
			}
		}
	} else {
		if len(rf.log) > 0 {
			lastEntry := rf.log[len(rf.log)-1]
			prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
		} else {
			prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
		}
	}

	reply := AppendEntriesReply{}
	args := AppendEntiesArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.id,
		PreviousLogIndex: prevLogIndex,
		PreviousLogTerm:  prevLogTerm,
		LogEntries:       entries,
		LeaderCommit:     rf.commitIndex,
	}

	rf.Unlock()

	ok := rf.sendAppendEntryRequest(peerIndex, &args, &reply)

	rf.Lock()
	tempState := rf.state
	tempisDecommissioned := rf.isDecommissioned
	tempcurrentTerm := rf.currentTerm
	rf.Unlock()

	if !ok {
		//RaftDebug("Communication error: AppendEntries() RPC failed", rf)
	} else if tempState != Leader || tempisDecommissioned || args.Term != tempcurrentTerm {
		RaftInfo("Node state has changed since request was sent. Discarding response", rf)
	} else if reply.Success {
		if len(entries) > 0 {
			RaftInfo("Appended %d entries to %s's log", rf, len(entries), peerId)
			lastReplicated := entries[len(entries)-1]

			rf.Lock()
			rf.matchIndex[peerIndex] = lastReplicated.Index
			rf.nextIndex[peerIndex] = lastReplicated.Index + 1
			rf.Unlock()

			rf.updateCommitIndex()
		} else {
			RaftDebug("Successful heartbeat from %s", rf, peerId)
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.Lock()
			RaftInfo("Switching to follower as %s's term is %d", rf, peerId, reply.Term)
			rf.transitionToFollower(reply.Term)
			rf.Unlock()
		} else {
			RaftInfo("Log deviation on %s. T: %d, nextIndex: %d, args.Prev[I: %d, T: %d], FirstConflictEntry[I: %d, T: %d]", rf, peerId, reply.Term, rf.nextIndex[peerIndex], args.PreviousLogIndex, args.PreviousLogTerm, reply.ConflictingLogIndex, reply.ConflictingLogTerm)
			// Log deviation, we should go back to `ConflictingLogIndex - 1`, lowest value for nextIndex[peerIndex] is 1.
			rf.nextIndex[peerIndex] = Max(reply.ConflictingLogIndex-1, 1)
			sendAppendChan <- struct{}{} // Signals to leader-peer process that appends need to occur
		}
	}

	rf.Lock()
	rf.persist()
	rf.Unlock()
}

func (rf *Raft) updateCommitIndex() {
	rf.Lock()
	defer rf.Unlock()

	// §5.3/5.4: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	for i := len(rf.log) - 1; i >= 0; i-- {
		if v := rf.log[i]; v.Term == rf.currentTerm && v.Index > rf.commitIndex {
			replicationCount := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= v.Index {
					if replicationCount++; replicationCount > len(rf.peers)/2 { // Check to see if majority of nodes have replicated this
						RaftInfo("Updating commit index [%d -> %d] as replication factor is at least: %d/%d", rf, rf.commitIndex, v.Index, replicationCount, len(rf.peers))
						rf.commitIndex = v.Index // Set index of this entry as new commit index
						break
					}
				}
			}
		} else {
			break
		}
	}
}

const HeartBeatInterval = 100 * time.Millisecond
const LeaderPeerTickInterval = 10 * time.Millisecond

func (rf *Raft) startLeaderPeerProcess(peerIndex int, sendAppendChan chan struct{}) {
	rf.Lock()
	RaftDebug("startLeaderPeerProcess from %s", rf, peerIndex)
	rf.Unlock()

	ticker := time.NewTicker(LeaderPeerTickInterval)
	// Initial heartbeat
	rf.sendAppendEntries(peerIndex, sendAppendChan)
	lastEntrySent := time.Now()

	for {
		rf.Lock()
		if rf.state != Leader || rf.isDecommissioned {
			ticker.Stop()
			rf.Unlock()
			break
		}
		rf.Unlock()

		select {
		case <-sendAppendChan: // Signal that we should send a new append to this peer
			lastEntrySent = time.Now()
			rf.sendAppendEntries(peerIndex, sendAppendChan)
		case currentTime := <-ticker.C: // If traffic has been idle, we should send a heartbeat
			if currentTime.Sub(lastEntrySent) >= HeartBeatInterval {
				lastEntrySent = time.Now()
				rf.sendAppendEntries(peerIndex, sendAppendChan)
			}
		}
	}
}

func (rf *Raft) getLastEntryInfo() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	return rf.lastSnapshotIndex, rf.lastSnapshotTerm
}

func (rf *Raft) findLogIndex(logIndex int) (int, bool) {
	for i, e := range rf.log {
		if logIndex == e.Index {
			return i, true
		}
	}
	return -1, false
}

func (rf *Raft) beginElection() {
	rf.Lock()

	rf.transitionToCandidate()
	lastIndex, lastTerm := rf.getLastEntryInfo()

	requestArg := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	replies := make([]RequestVoteReply, len(rf.peers))
	voteChan := make(chan int, len(rf.peers))

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(rf.peers[i], i, voteChan, &requestArg, &replies[i])
		}
	}

	rf.persist()
	rf.Unlock()

	votes := 1

	for i := 1; i < len(replies); i++ {
		reply := replies[<-voteChan]

		rf.Lock()

		if reply.Term > rf.currentTerm {
			RaftInfo("Switching to follower as %s's term is %d", rf, reply.Id, reply.Term)
			rf.transitionToFollower(reply.Term)
			rf.Unlock()
			break
		} else if votes += reply.VoteCount(); votes > len(replies)/2 {
			if rf.state == Candidate && requestArg.Term == rf.currentTerm {
				RaftInfo("Election won. Vote: %d/%d", rf, votes, len(rf.peers))
				rf.Unlock()
				go rf.promoteToLeader()
				break
			} else {
				// error, there is one bug here, it could be rf change status to follower already
				RaftInfo("Election for term %d was interrupted", rf, requestArg.Term)
				rf.Unlock()
				break
			}
		} else {
			rf.Unlock()
		}
	}

	rf.Lock()
	rf.persist()
	rf.Unlock()
}

const CommitApplyIdleCheckInterval = 25 * time.Millisecond

func (rf *Raft) startLocalApplyProcess(applyChan chan ApplyMsg) {
	for {
		rf.Lock()

		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {
			if rf.commitIndex < rf.lastSnapshotIndex {
				rf.Unlock()

				applyChan <- ApplyMsg{UseSnapshot: true, Snapshot: rf.persister.ReadSnapshot(), CommandValid: true}

				rf.Lock()
				rf.lastApplied = rf.lastSnapshotIndex
				rf.Unlock()
			} else {
				startIndex, _ := rf.findLogIndex(rf.lastApplied + 1)
				startIndex = Max(startIndex, 0) // If start index wasn't found, it's because it's a part of a snapshot

				endIndex := -1
				for i := startIndex; i < len(rf.log); i++ {
					if rf.log[i].Index <= rf.commitIndex {
						RaftDebug("### Test Command field %v index %d term %d", rf, rf.log[i].Command, rf.log[i].Index, rf.log[i].Term)
						endIndex = i
					}
				}

				if endIndex >= 0 { // We have some entries to locally commit
					entries := make([]LogEntry, endIndex-startIndex+1)
					copy(entries, rf.log[startIndex:endIndex+1])

					RaftInfo("### Locally applying %d log entries. lastApplied: %d. commitIndex: %d, startIndex:%d, endIndex: %d, len(rf.log): %d", rf, len(entries), rf.lastApplied, rf.commitIndex, startIndex, endIndex, len(rf.log))
					rf.Unlock()

					for _, v := range entries { // Hold no locks so that slow local applies don't deadlock the system
						RaftDebug("Locally applying log: %v", rf, v)
						applyChan <- ApplyMsg{CommandIndex: v.Index, Command: v.Command, CommandValid: true}
					}

					rf.Lock()
					rf.lastApplied += len(entries)
				}
				rf.Unlock()
			}
		} else {
			rf.Unlock()
			<-time.After(CommitApplyIdleCheckInterval)
		}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.id = string(rune(me + 'A'))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionProcess()

	go rf.startLocalApplyProcess(applyCh)

	return rf
}
