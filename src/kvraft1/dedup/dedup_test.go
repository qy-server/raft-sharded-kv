package dedup

import (
	"sync"
	"testing"

	"6.5840/kvsrv1/rpc"
)

func TestConcurrentSequencesAndAcknowledgements(t *testing.T) {
	var client Client
	const count = 100
	ids := make(chan uint64, count)
	sequences := make(chan uint64, count)
	var workers sync.WaitGroup
	for i := 0; i < count; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id, seq := client.Begin()
			ids <- id
			sequences <- seq
			if seq != 1 {
				client.Complete(seq)
			}
		}()
	}
	workers.Wait()
	close(ids)
	close(sequences)
	seen := map[uint64]bool{}
	var clientID uint64
	for id := range ids {
		if clientID == 0 {
			clientID = id
		}
		if id == 0 || id != clientID {
			t.Fatal("unstable client identity")
		}
	}
	for seq := range sequences {
		if seq == 0 || seen[seq] {
			t.Fatal("duplicate sequence")
		}
		seen[seq] = true
	}
	if client.Ack() != 0 {
		t.Fatal("acknowledged an unfinished request")
	}
	client.Complete(1)
	if client.Ack() != count || len(client.completed) != 0 {
		t.Fatal("acknowledgement did not advance")
	}
}

func TestOutOfOrderReplayAndRetirement(t *testing.T) {
	var table Table
	applied := 0
	apply := func() rpc.PutReply { applied++; return rpc.PutReply{Err: rpc.OK} }
	for _, seq := range []uint64{3, 1, 2, 3, 1, 2} {
		if got := table.Do(rpc.PutArgs{ClientID: 7, Sequence: seq}, apply); got.Err != rpc.OK {
			t.Fatal(got)
		}
	}
	if applied != 3 {
		t.Fatalf("applied %d times, want 3", applied)
	}
	table.Do(rpc.PutArgs{ClientID: 7, Sequence: 4, Ack: 3}, apply)
	if len(table.Sessions[7].Replies) != 1 {
		t.Fatal("acknowledged replies were retained")
	}
	if got := table.Do(rpc.PutArgs{ClientID: 7, Sequence: 2}, apply); got.Err != rpc.ErrStaleRequest {
		t.Fatal("retired request was accepted")
	}
	if applied != 4 {
		t.Fatal("retired request executed again")
	}
}

func TestFailureReplayAndLegacyRequests(t *testing.T) {
	var table Table
	args := rpc.PutArgs{ClientID: 7, Sequence: 1}
	want := rpc.PutReply{Err: rpc.ErrVersion}
	table.Do(args, func() rpc.PutReply { return want })
	got := table.Do(args, func() rpc.PutReply { t.Fatal("failed request re-executed"); return rpc.PutReply{} })
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := 0; i < 2; i++ {
		table.Do(rpc.PutArgs{}, func() rpc.PutReply { return rpc.PutReply{Err: rpc.OK} })
	}
	if len(table.Sessions) != 1 {
		t.Fatal("legacy clients allocated sessions")
	}
}
