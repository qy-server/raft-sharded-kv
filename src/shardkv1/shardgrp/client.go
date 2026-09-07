package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvraft1/dedup"
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const clerkRetryPause = 10 * time.Millisecond

type Clerk struct {
	*tester.Clnt
	servers []string

	mu       sync.Mutex
	leader   int // 最近一次请求成功的 Leader 在 servers 中的下标
	requests dedup.Client

	// 只用于 controller 迁移客户端；普通 Get/Put 客户端保持 nil。
	// 每次迁移 RPC 重试前检查，让失去领导权的 controller 退出旧任务。
	isCurrent func() bool
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	if len(servers) == 0 {
		panic("shardgrp: Clerk 至少需要一台服务器")
	}
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

// MakeMigrationClerk 保留普通 Clerk 的接口，并为迁移重试增加领导权检查。
// isCurrent 在调用 goroutine 内同步执行，不启动额外后台 goroutine。
func MakeMigrationClerk(clnt *tester.Clnt, servers []string, isCurrent func() bool) *Clerk {
	ck := MakeClerk(clnt, servers)
	ck.isCurrent = isCurrent
	return ck
}

func (ck *Clerk) Leader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) rememberLeader(server int) {
	ck.mu.Lock()
	ck.leader = server
	ck.mu.Unlock()
}

func (ck *Clerk) advanceLeader(failedServer int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if ck.leader == failedServer {
		ck.leader = (failedServer + 1) % len(ck.servers)
	}
}

// retryAfterFailure 轮询下一台服务器；完整尝试一轮后短暂退避，避免空转。
func (ck *Clerk) retryAfterFailure(failedServer int, failedAttempts *int) {
	ck.advanceLeader(failedServer)
	*failedAttempts = *failedAttempts + 1
	if *failedAttempts >= len(ck.servers) {
		*failedAttempts = 0
		time.Sleep(clerkRetryPause)
	}
}

// Get 优先访问缓存的 Leader，并且最多把当前组的服务器尝试一轮。
// 如果整组都不可用，则把控制权交回顶层 Clerk，让它刷新分片配置；
// 否则旧组退出后，客户端会永远困在已经失效的服务器列表中。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		receivedReply := false
		for attempts := 0; attempts < len(ck.servers); attempts++ {
			server := ck.Leader()
			reply := rpc.GetReply{}
			ok := ck.Clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)
			if ok {
				receivedReply = true
				switch reply.Err {
				case rpc.OK, rpc.ErrNoKey, rpc.ErrWrongGroup:
					ck.rememberLeader(server)
					return reply.Value, reply.Version, reply.Err
				}
			}
			ck.advanceLeader(server)
		}

		if !receivedReply {
			// 整个组都不可达，可能是配置中的旧组已经退出。
			time.Sleep(clerkRetryPause)
			return "", 0, rpc.ErrWrongLeader
		}
		// 服务器仍存活但正在选举，留在组内等待 Leader 产生。
		time.Sleep(clerkRetryPause)
	}
}

// Put 为直接访问分片组的客户端生成请求身份。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	args.ClientID, args.Sequence = ck.requests.Begin()
	defer ck.requests.Complete(args.Sequence)
	args.Ack = ck.requests.Ack()
	return ck.PutRequest(args)
}

// PutRequest preserves the outer Clerk's identity across group changes.
func (ck *Clerk) PutRequest(args rpc.PutArgs) rpc.Err {
	uncertain := false

	for {
		receivedReply := false
		for attempts := 0; attempts < len(ck.servers); attempts++ {
			server := ck.Leader()
			reply := rpc.PutReply{}
			ok := ck.Clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)
			if !ok {
				// 请求或响应可能丢失，无法确定该 Put 是否已经提交。
				uncertain = true
				ck.advanceLeader(server)
				continue
			}
			receivedReply = true

			switch reply.Err {
			case rpc.OK:
				ck.rememberLeader(server)
				return rpc.OK
			case rpc.ErrNoKey:
				ck.rememberLeader(server)
				return rpc.ErrNoKey
			case rpc.ErrVersion:
				ck.rememberLeader(server)
				if uncertain && args.ClientID == 0 {
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			case rpc.ErrStaleRequest:
				return rpc.ErrStaleRequest
			case rpc.ErrWrongGroup:
				ck.rememberLeader(server)
				// shard 已经迁走时，交给顶层 Clerk 在新组继续尝试。
				// 不能仅凭之前丢过响应就返回 ErrMaybe：之前的 Put 也可能
				// 根本没有执行，必须观察新组的版本后才能下结论。
				return rpc.ErrWrongGroup
			case rpc.ErrWrongLeader:
				// Leader 可能在提交请求后才失去领导权，因此结果也可能不确定。
				uncertain = true
			}

			ck.advanceLeader(server)
		}

		if !receivedReply {
			// 整组不可达时让顶层 Clerk 刷新配置，并在当前/新组继续
			// 尝试同一个版本的 Put。只有之后观察到 ErrVersion，才能
			// 把丢响应解释成 ErrMaybe。
			time.Sleep(clerkRetryPause)
			return rpc.ErrWrongLeader
		}
		// 仍能收到响应通常表示正在选举，继续等待而不是过早返回 ErrMaybe。
		time.Sleep(clerkRetryPause)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	failedAttempts := 0

	for {
		if ck.isCurrent != nil && !ck.isCurrent() {
			return nil, rpc.ErrWrongLeader
		}
		server := ck.Leader()
		reply := shardrpc.FreezeShardReply{}
		ok := ck.Clnt.Call(ck.servers[server], "KVServer.FreezeShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK:
				ck.rememberLeader(server)
				if reply.Num != num {
					return nil, rpc.ErrWrongGroup
				}
				return reply.State, rpc.OK
			case rpc.ErrWrongGroup:
				ck.rememberLeader(server)
				return nil, rpc.ErrWrongGroup
			}
		}
		ck.retryAfterFailure(server, &failedAttempts)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	failedAttempts := 0

	for {
		if ck.isCurrent != nil && !ck.isCurrent() {
			return rpc.ErrWrongLeader
		}
		server := ck.Leader()
		reply := shardrpc.InstallShardReply{}
		ok := ck.Clnt.Call(ck.servers[server], "KVServer.InstallShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrWrongGroup:
				ck.rememberLeader(server)
				return reply.Err
			}
		}
		ck.retryAfterFailure(server, &failedAttempts)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	failedAttempts := 0

	for {
		if ck.isCurrent != nil && !ck.isCurrent() {
			return rpc.ErrWrongLeader
		}
		server := ck.Leader()
		reply := shardrpc.DeleteShardReply{}
		ok := ck.Clnt.Call(ck.servers[server], "KVServer.DeleteShard", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrWrongGroup:
				ck.rememberLeader(server)
				return reply.Err
			}
		}
		ck.retryAfterFailure(server, &failedAttempts)
	}
}
