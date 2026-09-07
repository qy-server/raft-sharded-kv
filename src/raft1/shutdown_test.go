package raft

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

func TestKillStopsBlockedApplyAndRejectsWork(t *testing.T) {
	persister := tester.MakePersister()
	rf := Make(make([]*labrpc.ClientEnd, 1), 0, persister, make(chan raftapi.ApplyMsg)).(*Raft)
	defer rf.Kill()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, leader := rf.GetState(); leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no single-node leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	rf.Start("blocked-apply")
	time.Sleep(20 * time.Millisecond)
	var calls sync.WaitGroup
	for i := 0; i < 8; i++ {
		calls.Add(1)
		go func() { defer calls.Done(); rf.Kill() }()
	}
	calls.Wait()
	select {
	case <-rf.Stopped():
	case <-time.After(time.Second):
		t.Fatal("Raft workers did not stop with a blocked applyCh")
	}
	before := persister.ReadRaftState()
	if _, _, accepted := rf.Start("after-kill"); accepted {
		t.Fatal("Start accepted work after Kill")
	}
	if _, leader := rf.GetState(); leader {
		t.Fatal("killed node reports itself as leader")
	}
	var vote RequestVoteReply
	rf.RequestVote(&RequestVoteArgs{Term: 99, CandidateId: 1, LastLogTerm: 99}, &vote)
	if vote.VoteGranted {
		t.Fatal("killed node voted")
	}
	var appendReply AppendEntriesReply
	rf.AppendEntries(&AppendEntriesArgs{Term: 99}, &appendReply)
	if appendReply.Success || !appendReply.Stopped {
		t.Fatal("killed node accepted AppendEntries")
	}
	var snapshotReply InstallSnapshotReply
	rf.InstallSnapshot(&InstallSnapshotArgs{Term: 99, LastIncludedIndex: 99, LastIncludedTerm: 99}, &snapshotReply)
	if !snapshotReply.Stopped {
		t.Fatal("killed node acknowledged snapshot installation")
	}
	rf.Snapshot(1, []byte("after-kill"))
	if !bytes.Equal(before, persister.ReadRaftState()) {
		t.Fatal("persistent state changed after Kill")
	}
}

func TestKillStopsElectionAndReplicationWorkers(t *testing.T) {
	network := labrpc.MakeNetwork()
	defer network.Cleanup()
	peers := make([]*labrpc.ClientEnd, 3)
	for i := range peers {
		peers[i] = network.MakeEnd(i)
	}
	rf := Make(peers, 0, tester.MakePersister(), make(chan raftapi.ApplyMsg)).(*Raft)
	rf.mu.Lock()
	rf.electionDeadline = time.Time{}
	rf.mu.Unlock()
	rf.startElection()
	rf.Kill()
	select {
	case <-rf.Stopped():
	case <-time.After(5 * time.Second):
		t.Fatal("election or replication worker leaked after Kill")
	}
}
