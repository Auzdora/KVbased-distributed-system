package raft

const (
	// raft server state
	FOLLOWER  = 1 // a raft peer starts as a FOLLOWER, after a election timeout, it turn intro CANDIDATE
	CANDIDATE = 2 // a CANDIDATE will turn into LEADER if it recieves votes from majority of servers
	LEADER    = 3 // a LEADER will fail only the raft died or discovers server with higher term
)

func (rf *Raft) ToFollower(newTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = newTerm
	rf.ballot = 0
	rf.votedFor = -1
}

func (rf *Raft) ToCandidate() {
	rf.mu.Lock()
	// change meta info
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.ballot = 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// issue RequestVote RPC to other servers to get vote
	go rf.broadCastRequestVote()
}

func (rf *Raft) ToLeader() {
	rf.mu.Lock()
	rf.state = LEADER
	// rf.ballot = 0
	// rf.votedFor = -1
	select {
	case rf.appendCh <- true:
	default:
	}
	rf.mu.Unlock()
	// issue AppendEntries RPC to other servers to maintain authority
	go rf.broadCastHeartBeat()
}
