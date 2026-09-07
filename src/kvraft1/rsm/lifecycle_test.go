package rsm

import (
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/raftapi"
)

type waitingRaft struct{ started chan struct{} }

func (r *waitingRaft) Start(any) (int, int, bool) { close(r.started); return 1, 1, true }
func (*waitingRaft) GetState() (int, bool)        { return 1, true }
func (*waitingRaft) Snapshot(int, []byte)         {}
func (*waitingRaft) PersistBytes() int            { return 0 }

func TestSubmitTimeoutCleansWaiter(t *testing.T) {
	rsm := &RSM{rf: &waitingRaft{started: make(chan struct{})}, waiters: make(map[opID]chan applyResult), done: make(chan struct{})}
	if err, _ := rsm.SubmitWithTimeout("uncommitted", 20*time.Millisecond); err != rpc.ErrWrongLeader {
		t.Fatal(err)
	}
	if len(rsm.waiters) != 0 {
		t.Fatal("timed-out waiter leaked")
	}
}

func TestSubmitAndApplierStopWithRaft(t *testing.T) {
	stop := make(chan struct{})
	raft := &waitingRaft{started: make(chan struct{})}
	rsm := &RSM{rf: raft, waiters: make(map[opID]chan applyResult), done: make(chan struct{}), raftDone: stop, applyCh: make(chan raftapi.ApplyMsg)}
	go rsm.applier()
	result := make(chan rpc.Err, 1)
	go func() { err, _ := rsm.Submit("uncommitted"); result <- err }()
	<-raft.started
	close(stop)
	select {
	case err := <-result:
		if err != rpc.ErrWrongLeader {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Submit did not stop")
	}
	select {
	case <-rsm.done:
	case <-time.After(time.Second):
		t.Fatal("applier did not stop")
	}
	if len(rsm.waiters) != 0 {
		t.Fatal("stopped waiter leaked")
	}
}
