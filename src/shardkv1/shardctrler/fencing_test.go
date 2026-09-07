package shardctrler

import (
	"sync"
	"testing"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

// 内存 CAS 服务用于确定性地制造“请求成功但回复丢失”和过期写入。
type fencingKV struct {
	mu        sync.Mutex
	value     string
	version   rpc.Tversion
	loseReply bool
}

func (kv *fencingKV) Get(string) (string, rpc.Tversion, rpc.Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.version == 0 {
		return "", 0, rpc.ErrNoKey
	}
	return kv.value, kv.version, rpc.OK
}

func (kv *fencingKV) Put(_ string, value string, version rpc.Tversion) rpc.Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.version != version {
		return rpc.ErrVersion
	}
	kv.value = value
	kv.version++
	if kv.loseReply {
		kv.loseReply = false
		return rpc.ErrMaybe
	}
	return rpc.OK
}

func testController(kv *fencingKV, id string) *ShardCtrler {
	return &ShardCtrler{IKVClerk: kv, id: id}
}

// 新 owner 必须先完成旧 WAL；旧 owner 的迟到发布不能覆盖接管结果。
func TestControllerTakeoverResumesAndFences(t *testing.T) {
	kv := &fencingKV{}
	old := testController(kv, "old")
	cfg := shardcfg.MakeShardConfig()
	cfg.Num = shardcfg.NumFirst
	old.InitConfig(cfg)

	state, version, err := old.readState()
	if err != rpc.OK {
		t.Fatal(err)
	}
	target := cfg.Copy()
	target.Num++
	state.Pending = &migrationRecord{From: cfg.Copy(), To: target.Copy()}
	if !old.compareAndSwap(version, state) {
		t.Fatal("cannot persist migration")
	}
	stale, staleVersion, _ := old.readState()

	next := testController(kv, "next")
	next.InitController()
	recovered, _, _ := next.readState()
	if recovered.Owner != "next" || recovered.Epoch != 2 ||
		recovered.Current.Num != target.Num || recovered.Pending != nil {
		t.Fatalf("takeover did not finish pending migration: %+v", recovered)
	}
	stale.Current = target.Copy()
	if old.compareAndSwap(staleVersion, stale) {
		t.Fatal("stale owner published metadata after takeover")
	}

	// 已被接替的对象即使再次 Init/Change，也不能自动抢回领导权。
	want := next.Query().Copy()
	want.Num++
	old.InitController()
	old.ChangeConfigTo(want)
	final, _, _ := next.readState()
	if final.Owner != "next" || final.Current.Num != target.Num || !old.deposed {
		t.Fatal("deposed controller regained leadership")
	}
}

// 清除 WAL 与 owner 变更共享 CAS 版本，因此迟到的清理不能删除新 WAL。
func TestControllerStaleCleanupCannotEraseNewMigration(t *testing.T) {
	kv := &fencingKV{}
	old := testController(kv, "old")
	cfg := shardcfg.MakeShardConfig()
	cfg.Num = shardcfg.NumFirst
	old.InitConfig(cfg)
	stale, staleVersion, _ := old.readState()

	next := testController(kv, "next")
	next.InitController()
	fresh, version, _ := next.readState()
	target := fresh.Current.Copy()
	target.Num++
	fresh.Pending = &migrationRecord{From: fresh.Current.Copy(), To: target}
	if !next.compareAndSwap(version, fresh) {
		t.Fatal("cannot persist new migration")
	}
	stale.Pending = nil
	if old.compareAndSwap(staleVersion, stale) {
		t.Fatal("stale cleanup unexpectedly succeeded")
	}
	got, _, _ := next.readState()
	if got.Owner != "next" || got.Pending == nil || got.Pending.To.Num != target.Num {
		t.Fatal("new migration was erased")
	}
}

func TestControllerLostReplyAndReadOnlyQuery(t *testing.T) {
	kv := &fencingKV{loseReply: true}
	owner := testController(kv, "owner")
	cfg := shardcfg.MakeShardConfig()
	cfg.Num = shardcfg.NumFirst
	owner.InitConfig(cfg)
	if !owner.initialized {
		t.Fatal("successful initialization was lost with its reply")
	}

	_, before, _ := owner.readState()
	reader := testController(kv, "reader")
	got := reader.Query()
	got.Num = 999
	_, after, _ := owner.readState()
	if before != after || owner.Query().Num != cfg.Num || reader.initialized {
		t.Fatal("Query changed metadata or acquired leadership")
	}
}
