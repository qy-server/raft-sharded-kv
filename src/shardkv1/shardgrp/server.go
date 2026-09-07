package shardgrp

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"6.5840/kvraft1/dedup"
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

// shardStatus 表示当前副本组是否可以为某个 shard 提供服务。
// 冻结状态禁止新的读写，使迁移与普通操作共享同一条 Raft 顺序。
type shardStatus uint8

const (
	shardAbsent shardStatus = iota
	shardServing
	shardFrozen
)

// valueEntry 同时保存值和版本号，版本号用于实现条件写。
type valueEntry struct {
	Value   string
	Version rpc.Tversion
}

// shardState 把数据按 shard 分开保存，方便后续单独迁移和删除 shard。
// 字段需要导出，确保 labgob 能够正确编码快照。
type shardState struct {
	Values    map[string]valueEntry
	Status    shardStatus
	ConfigNum shardcfg.Tnum
	Requests  dedup.Table
}

// shardTransfer 是跨 group 复制的数据格式。版本号必须和 value 一起迁移，
// 否则目标组会错误地接受旧版本 Put。
type shardTransfer struct {
	Values   map[string]valueEntry
	Requests dedup.Table
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	mu     sync.Mutex
	shards [shardcfg.NShards]shardState
}

func encodeShardTransfer(values map[string]valueEntry, requests dedup.Table) []byte {
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	if err := encoder.Encode(shardTransfer{Values: values, Requests: requests}); err != nil {
		panic(fmt.Sprintf("shardgrp: 编码 shard 迁移数据失败：%v", err))
	}
	return buffer.Bytes()
}

func decodeShardTransfer(data []byte) shardTransfer {
	if len(data) == 0 {
		return shardTransfer{Values: make(map[string]valueEntry)}
	}

	var transfer shardTransfer
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&transfer); err != nil {
		panic(fmt.Sprintf("shardgrp: 解码 shard 迁移数据失败：%v", err))
	}
	if transfer.Values == nil {
		transfer.Values = make(map[string]valueEntry)
	}
	return transfer
}

// DoOp 是执行已提交 Raft 日志的唯一入口。Get 和 Put 都必须在这里
// 检查 shard 所有权，才能与后续的 Freeze 操作保持统一顺序。
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		shard := shardcfg.Key2Shard(args.Key)
		state := &kv.shards[shard]
		if state.Status != shardServing {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}

		entry, ok := state.Values[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{
			Value:   entry.Value,
			Version: entry.Version,
			Err:     rpc.OK,
		}

	case rpc.PutArgs:
		shard := shardcfg.Key2Shard(args.Key)
		state := &kv.shards[shard]
		if state.Status != shardServing {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}

		return state.Requests.Do(args, func() rpc.PutReply {
			entry, exists := state.Values[args.Key]
			if !exists && args.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			if entry.Version != args.Version {
				return rpc.PutReply{Err: rpc.ErrVersion}
			}
			entry.Value = args.Value
			entry.Version++
			state.Values[args.Key] = entry
			return rpc.PutReply{Err: rpc.OK}
		})

	case shardrpc.FreezeShardArgs:
		state := &kv.shards[args.Shard]

		// 相同配置的重复 Freeze 返回相同的冻结数据。
		if args.Num == state.ConfigNum && state.Status == shardFrozen {
			return shardrpc.FreezeShardReply{
				State: encodeShardTransfer(state.Values, state.Requests),
				Num:   state.ConfigNum,
				Err:   rpc.OK,
			}
		}

		// 只有当前正在服务的 shard 才能进入一个更新配置的冻结状态。
		if state.Status != shardServing || args.Num <= state.ConfigNum {
			return shardrpc.FreezeShardReply{
				Num: state.ConfigNum,
				Err: rpc.ErrWrongGroup,
			}
		}

		state.Status = shardFrozen
		state.ConfigNum = args.Num
		return shardrpc.FreezeShardReply{
			State: encodeShardTransfer(state.Values, state.Requests),
			Num:   state.ConfigNum,
			Err:   rpc.OK,
		}

	case shardrpc.InstallShardArgs:
		state := &kv.shards[args.Shard]

		// 已经完成的同配置 Install 是幂等成功，不能再次覆盖数据。
		if args.Num == state.ConfigNum && state.Status == shardServing {
			return shardrpc.InstallShardReply{Err: rpc.OK}
		}
		if state.Status != shardAbsent || args.Num <= state.ConfigNum {
			return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
		}

		transfer := decodeShardTransfer(args.State)
		state.Values = transfer.Values
		state.Requests = transfer.Requests
		state.Status = shardServing
		state.ConfigNum = args.Num
		return shardrpc.InstallShardReply{Err: rpc.OK}

	case shardrpc.DeleteShardArgs:
		state := &kv.shards[args.Shard]

		// 更旧的 Delete 或相同配置的重复 Delete 都是安全的空操作。
		if args.Num < state.ConfigNum ||
			(args.Num == state.ConfigNum && state.Status == shardAbsent) {
			return shardrpc.DeleteShardReply{Err: rpc.OK}
		}
		if args.Num != state.ConfigNum || state.Status != shardFrozen {
			return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
		}

		state.Values = make(map[string]valueEntry)
		state.Requests = dedup.Table{}
		state.Status = shardAbsent
		return shardrpc.DeleteShardReply{Err: rpc.OK}

	default:
		panic(fmt.Sprintf("shardgrp: 不支持的操作类型 %T", req))
	}
}

// Snapshot 保存全部 shard 的数据、版本、服务状态和配置编号。
func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	if err := encoder.Encode(kv.shards); err != nil {
		panic(fmt.Sprintf("shardgrp: 编码快照失败：%v", err))
	}
	return buffer.Bytes()
}

// Restore 从快照恢复完整的分片状态。先解码到临时变量，成功后再替换
// 当前状态，避免用不完整的数据破坏正在使用的状态机。
func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}

	var shards [shardcfg.NShards]shardState
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&shards); err != nil {
		panic(fmt.Sprintf("shardgrp: 解码快照失败：%v", err))
	}
	for shard := range shards {
		if shards[shard].Values == nil {
			shards[shard].Values = make(map[string]valueEntry)
		}
	}

	kv.mu.Lock()
	kv.shards = shards
	kv.mu.Unlock()
}

// Get 将读请求提交给 Raft，不能直接读取 Leader 的本地 map。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, result := kv.rsm.SubmitWithTimeout(*args, time.Second)
	if err != rpc.OK {
		*reply = rpc.GetReply{Err: err}
		return
	}

	resultReply, ok := result.(rpc.GetReply)
	if !ok {
		panic(fmt.Sprintf("shardgrp: Get 收到错误的执行结果类型 %T", result))
	}
	*reply = resultReply
}

// Put 将条件写请求提交给 Raft，并返回状态机执行后的结果。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.SubmitWithTimeout(*args, time.Second)
	if err != rpc.OK {
		*reply = rpc.PutReply{Err: err}
		return
	}

	resultReply, ok := result.(rpc.PutReply)
	if !ok {
		panic(fmt.Sprintf("shardgrp: Put 收到错误的执行结果类型 %T", result))
	}
	*reply = resultReply
}

// FreezeShard 冻结指定 shard，使后续 Get/Put 返回 ErrWrongGroup，
// 并返回该 shard 的全部 value 和 version。
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, result := kv.rsm.SubmitWithTimeout(*args, time.Second)
	if err != rpc.OK {
		*reply = shardrpc.FreezeShardReply{Err: err}
		return
	}

	resultReply, ok := result.(shardrpc.FreezeShardReply)
	if !ok {
		panic(fmt.Sprintf("shardgrp: FreezeShard 收到错误的执行结果类型 %T", result))
	}
	*reply = resultReply
}

// InstallShard 把迁移数据安装到目标组，并开始为该 shard 提供服务。
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, result := kv.rsm.SubmitWithTimeout(*args, time.Second)
	if err != rpc.OK {
		*reply = shardrpc.InstallShardReply{Err: err}
		return
	}

	resultReply, ok := result.(shardrpc.InstallShardReply)
	if !ok {
		panic(fmt.Sprintf("shardgrp: InstallShard 收到错误的执行结果类型 %T", result))
	}
	*reply = resultReply
}

// DeleteShard 在新配置提交后删除源组中的旧副本。
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, result := kv.rsm.SubmitWithTimeout(*args, time.Second)
	if err != rpc.OK {
		*reply = shardrpc.DeleteShardReply{Err: err}
		return
	}

	resultReply, ok := result.(shardrpc.DeleteShardReply)
	if !ok {
		panic(fmt.Sprintf("shardgrp: DeleteShard 收到错误的执行结果类型 %T", result))
	}
	*reply = resultReply
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 注册所有会被放入 interface 并经过 RPC/Raft 日志编码的具体类型。
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	for shard := range kv.shards {
		kv.shards[shard] = shardState{
			Values: make(map[string]valueEntry),
			Status: shardAbsent,
		}
		// 实验约定：初始的 Gid1 启动时拥有全部 shard。
		if gid == shardcfg.Gid1 {
			kv.shards[shard].Status = shardServing
			kv.shards[shard].ConfigNum = shardcfg.NumFirst
		}
	}

	// MakeRSM 可能立即调用 Restore，因此必须先初始化状态机数据。
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
