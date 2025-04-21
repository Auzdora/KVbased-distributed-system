package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// PrevLogIndex int
	// PrevLogTerm  int
	// Entries      []string
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	// fmt.Println("server", rf.me, "get request vote rpc from", args.CandidateId)
	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if rf.votedFor == -1 {
		// fmt.Println("server", rf.me, "VOTE FOR", args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		select {
		case <-rf.voteCh:
		default:
		}
		rf.voteCh <- true
	}
	rf.mu.Unlock()
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		rf.ToFollower(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	select {
	case <-rf.appendCh:
	default:
	}
	rf.appendCh <- true
	rf.mu.Unlock()
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
	// fmt.Println("server", rf.me, "send request vote rpc to", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.ToFollower(reply.Term)
	}

	if rf.state == FOLLOWER || !reply.VoteGranted {
		rf.mu.Unlock()
		return false
	}

	if rf.state == CANDIDATE {
		// fmt.Println("server", rf.me, "current state is", rf.state)
		if reply.VoteGranted {
			rf.ballot += 1
		}
		if rf.ballot > len(rf.peers)/2 {
			// win election and become a leader
			// fmt.Println("server", rf.me, "become a LEADER")
			rf.mu.Unlock()
			rf.ToLeader()
			rf.mu.Lock()
		}
	}
	rf.mu.Unlock()

	return ok
}

func (rf *Raft) sendApendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.ToFollower(reply.Term)
	}

	if rf.state == FOLLOWER || !reply.Success {
		return false
	}

	return ok
}

func (rf *Raft) broadCastRequestVote() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0, // TODO: to be implemented
		LastLogTerm:  0, // TODO: to be implemented
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
}

func (rf *Raft) broadCastHeartBeat() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		go rf.sendApendEntries(i, &args, &reply)
	}
}
