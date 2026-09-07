package kvraft

import (
	"bytes"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
)

func TestDedupSnapshotAndCachedResults(t *testing.T) {
	kv := &KVServer{values: make(map[string]valueEntry)}
	args := rpc.PutArgs{Key: "x", Value: "first", ClientID: 11, Sequence: 1}
	for i := 0; i < 3; i++ {
		if got := kv.DoOp(args).(rpc.PutReply); got.Err != rpc.OK {
			t.Fatal(got)
		}
	}
	restored := &KVServer{}
	restored.Restore(kv.Snapshot())
	restored.DoOp(rpc.PutArgs{Key: "x", Value: "new", Version: 1, ClientID: 22, Sequence: 1})
	if got := restored.DoOp(args).(rpc.PutReply); got.Err != rpc.OK {
		t.Fatal("lost cached success", got)
	}
	if got := restored.values["x"]; got.Value != "new" || got.Version != 2 {
		t.Fatal("replay changed state", got)
	}
	failed := rpc.PutArgs{Key: "x", Version: 99, ClientID: 11, Sequence: 2}
	if got := restored.DoOp(failed).(rpc.PutReply); got.Err != rpc.ErrVersion {
		t.Fatal(got)
	}
	failed.Version = 2
	if got := restored.DoOp(failed).(rpc.PutReply); got.Err != rpc.ErrVersion {
		t.Fatal("lost cached failure", got)
	}
	retire := rpc.PutArgs{Key: "x", Value: "last", Version: 2, ClientID: 11, Sequence: 3, Ack: 2}
	restored.DoOp(retire)
	restored.Restore(restored.Snapshot())
	if got := restored.DoOp(args).(rpc.PutReply); got.Err != rpc.ErrStaleRequest {
		t.Fatal("lost ack watermark", got)
	}
}

func TestDedupLegacySnapshot(t *testing.T) {
	var data bytes.Buffer
	if err := labgob.NewEncoder(&data).Encode(map[string]valueEntry{"old": {Value: "value", Version: 5}}); err != nil {
		t.Fatal(err)
	}
	kv := &KVServer{}
	kv.Restore(data.Bytes())
	if got := kv.values["old"]; got.Version != 5 || got.Value != "value" {
		t.Fatal(got)
	}
}

func TestDedupReplyLostAcrossLeaderFailure(t *testing.T) {
	ts := MakeTest(t, "dedup reply loss", 0, 3, true, false, false, 1000, false)
	defer ts.Cleanup()
	client := ts.Config.MakeClient()
	args := rpc.PutArgs{Key: "lost-reply", Value: "once", ClientID: 93, Sequence: 1}
	call := func(exclude int) int {
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			for peer, name := range ts.Group(Gid).SrvNames() {
				if peer == exclude {
					continue
				}
				var reply rpc.PutReply
				if client.Call(name, "KVServer.Put", &args, &reply) && reply.Err == rpc.OK {
					return peer
				}
			}
		}
		t.Fatal("write did not obtain its original successful reply")
		return -1
	}
	leader := call(-1)
	// Discard the successful reply, then retry the identical request elsewhere.
	ts.Group(Gid).ShutdownServer(leader)
	call(leader)
	value, version, err := ts.MakeClerk().Get(args.Key)
	if err != rpc.OK || value != args.Value || version != 1 {
		t.Fatalf("got %q version %d: %s", value, version, err)
	}
}
