package raft

import "time"

type LogEntry struct {
	// the term when the entry was received by the leader
	Term int

	// command to be applied
	Command interface{}

	// the index of the log entry
	CommandIndex int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// used to probe the match index
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// return failure if prevLog doesn't match
	if args.PrevLogIndex > len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}

	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// append the leader logs to local
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
	reply.Success = true

	// TODO(Wenlong Dong): handle the args.LeaderCommit

	// reset the election timer, promising not start election in some interval
	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		// align term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// handle the reply
		// probe the lower index if the prev log not matched
		if !reply.Success {
			idx := rf.nextIndex[peer] - 1
			term := rf.log[idx].Term

			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}

			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Log not matched in %d, Update next=%d", args.PrevLogIndex, rf.nextIndex[peer])
			return
		}
		// update the match/next index if log appended successfully
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// TODO(Wenlong Dong): need compute the new commitIndex here
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.nextIndex[peer] = len(rf.log)
			rf.matchIndex[peer] = len(rf.log) - 1
			continue
		}

		prevLogIndex := rf.nextIndex[peer] - 1
		prevLogTerm := rf.log[prevLogIndex].Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.log[prevLogIndex+1:],
		}

		go replicateToPeer(peer, args)
	}

	return true
}

func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}

		time.Sleep(replicateInterval)
	}
}
