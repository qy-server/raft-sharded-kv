package shardgrp

import (
	"testing"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

func TestDedupSurvivesMigrationAndRestore(t *testing.T) {
	key := "migration-retry"
	shard := shardcfg.Key2Shard(key)
	source := &KVServer{}
	source.shards[shard] = shardState{Status: shardServing, ConfigNum: 1, Values: make(map[string]valueEntry)}
	args := rpc.PutArgs{Key: key, Value: "first", ClientID: 7, Sequence: 1}
	if got := source.DoOp(args).(rpc.PutReply); got.Err != rpc.OK {
		t.Fatal(got)
	}
	frozen := source.DoOp(shardrpc.FreezeShardArgs{Shard: shard, Num: 2}).(shardrpc.FreezeShardReply)
	if frozen.Err != rpc.OK {
		t.Fatal(frozen)
	}
	if got := source.DoOp(args).(rpc.PutReply); got.Err != rpc.ErrWrongGroup {
		t.Fatal("frozen shard served a write")
	}
	target := &KVServer{}
	if got := target.DoOp(shardrpc.InstallShardArgs{Shard: shard, Num: 2, State: frozen.State}).(shardrpc.InstallShardReply); got.Err != rpc.OK {
		t.Fatal(got)
	}
	source.DoOp(shardrpc.DeleteShardArgs{Shard: shard, Num: 2})
	restored := &KVServer{}
	restored.Restore(target.Snapshot())
	if got := restored.DoOp(args).(rpc.PutReply); got.Err != rpc.OK {
		t.Fatal("migration lost cached result", got)
	}
	if restored.shards[shard].Values[key].Version != 1 {
		t.Fatal("duplicate write changed the version")
	}
	newer := rpc.PutArgs{Key: key, Value: "new", Version: 1, ClientID: 7, Sequence: 3}
	if got := restored.DoOp(newer).(rpc.PutReply); got.Err != rpc.OK {
		t.Fatal(got)
	}
	newer.Sequence, newer.Version, newer.Value = 2, 2, "out-of-order"
	if got := restored.DoOp(newer).(rpc.PutReply); got.Err != rpc.OK {
		t.Fatal("out-of-order request was dropped", got)
	}
	if got := restored.DoOp(args).(rpc.PutReply); got.Err != rpc.OK {
		t.Fatal(got)
	}
	if got := restored.shards[shard].Values[key]; got.Version != 3 || got.Value != "out-of-order" {
		t.Fatal(got)
	}
}
