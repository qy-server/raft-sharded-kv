package shardctrler

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const currentConfigKey = "shardctrler/current-config"

// migrationRecord 保存迁移前后的配置；发布新配置后仍需 From 找到旧副本。
type migrationRecord struct {
	From *shardcfg.ShardConfig
	To   *shardcfg.ShardConfig
}

// controllerState 整体存入同一个 KV 键。任期、当前配置和 WAL 必须一起
// CAS，避免“检查完领导权后，旧 controller 又写入另一个键”的竞态。
// Epoch 是接管编号，Num 是分片配置号，KV version 是 CAS 版本，三者不同。
type controllerState struct {
	Owner   string
	Epoch   uint64
	Current *shardcfg.ShardConfig
	Pending *migrationRecord
}

type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	// 只串行化本对象的初始化和配置变更；Query 不获取这把锁。
	changeMu    sync.Mutex
	id          string
	epoch       uint64
	initialized bool
	deposed     bool // 一旦被接替，本对象就不能自动抢回领导权
}

func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	return &ShardCtrler{
		clnt:     clnt,
		id:       tester.Randstring(20),
		IKVClerk: kvsrv.MakeClerk(clnt, tester.ServerName(tester.GRP0, 0)),
	}
}

// InitConfig 只用于实验初始配置。当前配置、空 WAL 和初始 owner 原子创建。
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	if cfg == nil {
		return
	}
	sck.changeMu.Lock()
	defer sck.changeMu.Unlock()

	state := &controllerState{Owner: sck.id, Epoch: 1, Current: cfg.Copy()}
	if sck.compareAndSwap(0, state) {
		sck.epoch = state.Epoch
		sck.initialized = true
	}
}

// InitController 用 CAS 接管领导权并完成遗留迁移。接管只更换 Owner/Epoch，
// 不能更换已经选定的 Pending，否则同一配置号可能迁往两个不同目标组。
func (sck *ShardCtrler) InitController() {
	sck.changeMu.Lock()
	defer sck.changeMu.Unlock()
	if sck.ensureLeadershipLocked() {
		sck.recoverPendingLocked()
	}
}

// ensureLeadershipLocked 仅允许新 controller 参加一次竞选。失效对象再被
// 调用 ChangeConfigTo 时直接退出，防止旧 goroutine 重连后反复抢占。
func (sck *ShardCtrler) ensureLeadershipLocked() bool {
	if sck.initialized {
		return sck.isCurrent()
	}
	sck.initialized = true
	for {
		state, version, err := sck.readState()
		if err != rpc.OK {
			sck.deposed = true
			return false
		}
		state.Owner = sck.id
		state.Epoch++
		if sck.compareAndSwap(version, state) {
			sck.epoch = state.Epoch
			return true
		}
		// 有其他候选者同时写入，读取最新版本后重新竞争。
		time.Sleep(10 * time.Millisecond)
	}
}

// readOwned 同时检查持久化的 Owner 和 Epoch。调用方持有 changeMu。
func (sck *ShardCtrler) readOwned() (*controllerState, rpc.Tversion, bool) {
	if sck.deposed {
		return nil, 0, false
	}
	state, version, err := sck.readState()
	if err != rpc.OK {
		return nil, 0, false
	}
	if state.Owner != sck.id || state.Epoch != sck.epoch {
		sck.deposed = true
		return nil, 0, false
	}
	return state, version, true
}

func (sck *ShardCtrler) isCurrent() bool {
	_, _, ok := sck.readOwned()
	return ok
}

// ChangeConfigTo 先恢复旧任务，再原子选定新任务，最后执行迁移。
// 新配置号只有一个不可变的 From/To；所有接管者都必须完成同一计划。
func (sck *ShardCtrler) ChangeConfigTo(next *shardcfg.ShardConfig) {
	if next == nil {
		return
	}
	sck.changeMu.Lock()
	defer sck.changeMu.Unlock()
	if !sck.ensureLeadershipLocked() || !sck.recoverPendingLocked() {
		return
	}

	state, version, ok := sck.readOwned()
	if !ok || reflect.DeepEqual(state.Current, next) {
		return
	}
	if next.Num != state.Current.Num+1 {
		return // 调用者的配置已过期，不能重用旧配置号
	}
	state.Pending = &migrationRecord{From: state.Current.Copy(), To: next.Copy()}
	// 成功持久化 WAL 之后，才允许向任何 shard 发送迁移 RPC。
	if sck.compareAndSwap(version, state) {
		sck.recoverPendingLocked()
	}
}

// recoverPendingLocked 重放幂等迁移。元数据 CAS 和 shard 的配置号共同
// 提供 fencing：旧 owner 不能发布/清理新元数据，延迟 RPC 不能回滚 shard。
func (sck *ShardCtrler) recoverPendingLocked() bool {
	state, version, ok := sck.readOwned()
	if !ok || state.Pending == nil {
		return ok
	}
	record := state.Pending
	clerks := make(map[tester.Tgid]*shardgrp.Clerk)
	groupClerk := func(gid tester.Tgid, groups map[tester.Tgid][]string) *shardgrp.Clerk {
		if ck, ok := clerks[gid]; ok {
			return ck
		}
		servers := groups[gid]
		if len(servers) == 0 {
			panic(fmt.Sprintf("shardctrler: 配置中缺少 group %d 的服务器", gid))
		}
		ck := shardgrp.MakeMigrationClerk(sck.clnt, append([]string(nil), servers...), sck.isCurrent)
		clerks[gid] = ck
		return ck
	}

	if reflect.DeepEqual(state.Current, record.From) {
		for i, source := range record.From.Shards {
			target := record.To.Shards[i]
			if source == target {
				continue
			}
			shard := shardcfg.Tshid(i)
			var data []byte
			if source != 0 {
				var err rpc.Err
				data, err = groupClerk(source, record.From.Groups).FreezeShard(shard, record.To.Num)
				if err != rpc.OK {
					return false
				}
			}
			if target != 0 {
				if err := groupClerk(target, record.To.Groups).InstallShard(shard, data, record.To.Num); err != rpc.OK {
					return false
				}
			}
		}
		state.Current = record.To.Copy()
		// 使用迁移开始时的版本。即使旧 owner 的 RPC 刚刚成功，接管操作
		// 已增加 KV version，这个 CAS 仍会拒绝旧 owner 发布配置。
		if !sck.compareAndSwap(version, state) {
			return false
		}
	}

	// 发布成功后读回最新 CAS 版本，并确认领导权仍属于自己。
	state, version, ok = sck.readOwned()
	if !ok || !reflect.DeepEqual(state.Pending, record) || !reflect.DeepEqual(state.Current, record.To) {
		return false
	}
	for i, source := range record.From.Shards {
		if source == 0 || source == record.To.Shards[i] {
			continue
		}
		if err := groupClerk(source, record.From.Groups).DeleteShard(shardcfg.Tshid(i), record.To.Num); err != rpc.OK {
			return false
		}
	}
	// 删除全部旧副本后才清 WAL；接管者若已改写 Owner，这次 CAS 会失败。
	state.Pending = nil
	return sck.compareAndSwap(version, state)
}

// compareAndSwap 在丢响应时读回完整记录确认。不能仅比较 Current.Num：
// 接管后的配置号可能相同，但 Owner/Epoch 已经改变。
func (sck *ShardCtrler) compareAndSwap(version rpc.Tversion, state *controllerState) bool {
	data, err := json.Marshal(state)
	if err != nil {
		panic(fmt.Sprintf("shardctrler: 编码控制状态失败：%v", err))
	}
	putErr := sck.Put(currentConfigKey, string(data), version)
	if putErr == rpc.OK {
		return true
	}
	if putErr != rpc.ErrMaybe {
		return false
	}
	stored, _, readErr := sck.readState()
	return readErr == rpc.OK && reflect.DeepEqual(stored, state)
}

func (sck *ShardCtrler) readState() (*controllerState, rpc.Tversion, rpc.Err) {
	value, version, err := sck.Get(currentConfigKey)
	if err != rpc.OK {
		return nil, version, err
	}
	state := &controllerState{}
	if err := json.Unmarshal([]byte(value), state); err != nil || state.Current == nil {
		panic("shardctrler: 控制状态无法解码或缺少 Current 配置")
	}
	return state, version, rpc.OK
}

// Query 只读 Current，既不竞选，也不等待迁移锁，稳定 shard 可继续服务。
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	state, _, err := sck.readState()
	if err != rpc.OK {
		return nil
	}
	return state.Current.Copy()
}
