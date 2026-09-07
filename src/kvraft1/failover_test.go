package kvraft

import (
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

// A client can reach every server even when the servers cannot reach each other.
func TestClientRecoversFromReachableOldLeader(t *testing.T) {
	ts := MakeTest(t, "review reachable old leader", 0, 3, true, false, false, -1, false)
	defer ts.Cleanup()
	ck := ts.MakeClerk()
	if err := ck.Put("review-key", "initial", 0); err != rpc.OK {
		t.Fatal(err)
	}
	leader := ck.(*kvtest.TestClerk).IKVClerk.(*Clerk).Leader()
	majority := []int{}
	for peer := 0; peer < 3; peer++ {
		if peer != leader {
			majority = append(majority, peer)
		}
	}
	ts.Group(Gid).Partition(majority, []int{leader})
	defer ts.Group(Gid).ConnectAll()
	majorityClerk := ts.MakeClerkTo(majority)
	if value, _, err := majorityClerk.Get("review-key"); err != rpc.OK || value != "initial" {
		t.Fatalf("majority read failed: %q, %s", value, err)
	}
	t.Log("A new leader in the majority can serve requests; client still caches the isolated old leader.")
	done := make(chan rpc.Err, 1)
	go func() {
		_, _, err := ck.Get("review-key")
		done <- err
	}()
	select {
	case err := <-done:
		if err != rpc.OK {
			t.Fatal(err)
		}
		return
	case <-time.After(3 * time.Second):
	}
	// Heal before reporting the failure so the waiting RPC can finish cleanly.
	ts.Group(Gid).ConnectAll()
	select {
	case err := <-done:
		t.Errorf("client stayed on the reachable isolated old leader for 3s despite a working majority; returned %s only after healing", err)
	case <-time.After(5 * time.Second):
		t.Error("client did not finish within 5s of healing")
	}
}
