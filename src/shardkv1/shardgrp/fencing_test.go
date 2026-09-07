package shardgrp

import (
	"testing"

	"6.5840/kvraft1/dedup"
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

func TestShardFencingAfterReinstallAndRestore(t *testing.T) {
	const key = "fencing-key"
	shard := shardcfg.Key2Shard(key)
	kv := &KVServer{}
	kv.shards[shard] = shardState{
		Status: shardServing, ConfigNum: 1,
		Values: map[string]valueEntry{key: {Value: "old", Version: 1}},
	}
	frozen := kv.DoOp(shardrpc.FreezeShardArgs{Shard: shard, Num: 2}).(shardrpc.FreezeShardReply)
	if frozen.Err != rpc.OK {
		t.Fatal(frozen.Err)
	}
	if rep := kv.DoOp(shardrpc.DeleteShardArgs{Shard: shard, Num: 2}).(shardrpc.DeleteShardReply); rep.Err != rpc.OK {
		t.Fatal(rep.Err)
	}

	// 快照恢复后必须保留 absent + 配置号，防止旧 Install 复活已删除数据。
	restored := &KVServer{}
	restored.Restore(kv.Snapshot())
	if rep := restored.DoOp(shardrpc.InstallShardArgs{Shard: shard, Num: 2, State: frozen.State}).(shardrpc.InstallShardReply); rep.Err != rpc.ErrWrongGroup {
		t.Fatal("restored tombstone accepted stale install")
	}

	newData := encodeShardTransfer(map[string]valueEntry{key: {Value: "new", Version: 7}}, dedup.Table{})
	if rep := restored.DoOp(shardrpc.InstallShardArgs{Shard: shard, Num: 3, State: newData}).(shardrpc.InstallShardReply); rep.Err != rpc.OK {
		t.Fatal(rep.Err)
	}
	if rep := restored.DoOp(rpc.PutArgs{Key: key, Value: "latest", Version: 7}).(rpc.PutReply); rep.Err != rpc.OK {
		t.Fatal(rep.Err)
	}
	// 相同迁移的重发不能覆盖安装之后的新写入。
	restored.DoOp(shardrpc.InstallShardArgs{Shard: shard, Num: 3, State: newData})
	restored.DoOp(shardrpc.FreezeShardArgs{Shard: shard, Num: 2})
	restored.DoOp(shardrpc.InstallShardArgs{Shard: shard, Num: 2, State: frozen.State})
	restored.DoOp(shardrpc.DeleteShardArgs{Shard: shard, Num: 2})
	got := restored.DoOp(rpc.GetArgs{Key: key}).(rpc.GetReply)
	if got.Err != rpc.OK || got.Value != "latest" || got.Version != 8 {
		t.Fatalf("late migration RPC damaged current shard: %+v", got)
	}
}

func TestMigrationClerkStopsWhenSuperseded(t *testing.T) {
	// nil Clnt 保证如果仍然发出 RPC，测试会立即失败。
	ck := MakeMigrationClerk(nil, []string{"unreachable"}, func() bool { return false })
	if _, err := ck.FreezeShard(0, 2); err != rpc.ErrWrongLeader {
		t.Fatal(err)
	}
	if err := ck.InstallShard(0, nil, 2); err != rpc.ErrWrongLeader {
		t.Fatal(err)
	}
	if err := ck.DeleteShard(0, 2); err != rpc.ErrWrongLeader {
		t.Fatal(err)
	}
}
