package shardkv

//
// 用于访问分片键值服务的客户端代码。
//
// 客户端通过 shardctrler 查询当前配置，找到 key 所在 shard 对应的 group，
// 然后直接访问拥有该 shard 的 group。
//

import (
	"sync"
	"time"

	"6.5840/kvraft1/dedup"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const routeRetryPause = 10 * time.Millisecond

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler

	mu         sync.Mutex
	rcks       map[tester.Tgid]*shardgrp.Clerk
	rckServers map[tester.Tgid][]string
	config     *shardcfg.ShardConfig
	requests   dedup.Client
}

// 测试程序传入 shardctrler，客户端通过它的 Query 方法获得路由配置。
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:       clnt,
		sck:        sck,
		rcks:       make(map[tester.Tgid]*shardgrp.Clerk),
		rckServers: make(map[tester.Tgid][]string),
	}
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	rck, ok := ck.rcks[gid]
	return rck, ok
}

func sameServers(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// groupClerk 返回 gid 对应的 Clerk。若服务器列表发生变化，则重建 Clerk，
// 避免一直使用旧配置中的 RPC 地址。
func (ck *Clerk) groupClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if rck, ok := ck.rcks[gid]; ok && sameServers(ck.rckServers[gid], servers) {
		return rck
	}

	serverCopy := append([]string(nil), servers...)
	rck := shardgrp.MakeClerk(ck.clnt, serverCopy)
	ck.rcks[gid] = rck
	ck.rckServers[gid] = serverCopy
	return rck
}

// loadConfig 返回 Clerk 缓存的最新配置。普通请求复用缓存；收到
// ErrWrongGroup 后传入 force=true，重新向 controller 查询。
func (ck *Clerk) loadConfig(force bool) *shardcfg.ShardConfig {
	if !force {
		ck.mu.Lock()
		cached := ck.config
		ck.mu.Unlock()
		if cached != nil {
			return cached
		}
	}

	queried := ck.sck.Query()
	if queried == nil {
		return nil
	}
	queried = queried.Copy()

	ck.mu.Lock()
	// 并发刷新时不允许较旧配置覆盖已经缓存的较新配置。
	if ck.config == nil || queried.Num >= ck.config.Num {
		ck.config = queried
	}
	current := ck.config
	ck.mu.Unlock()
	return current
}

// Get 根据 key 计算 shard，再按照 controller 的当前配置找到对应 group。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shard := shardcfg.Key2Shard(key)
	forceRefresh := false

	for {
		cfg := ck.loadConfig(forceRefresh)
		forceRefresh = false
		if cfg == nil {
			forceRefresh = true
			time.Sleep(routeRetryPause)
			continue
		}

		_, servers, ok := cfg.GidServers(shard)
		if !ok || len(servers) == 0 {
			forceRefresh = true
			time.Sleep(routeRetryPause)
			continue
		}
		gid := cfg.Shards[shard]
		rck := ck.groupClerk(gid, servers)
		value, version, err := rck.Get(key)
		switch err {
		case rpc.OK, rpc.ErrNoKey:
			return value, version, err
		case rpc.ErrWrongGroup, rpc.ErrWrongLeader:
			// shard 已经迁走，或旧组整体不可达；刷新配置后重新路由。
			forceRefresh = true
			time.Sleep(routeRetryPause)
		}
	}
}

// Put 使用与 Get 相同的路由过程；路由失效时刷新配置重试。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)
	forceRefresh := false
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	args.ClientID, args.Sequence = ck.requests.Begin()
	defer ck.requests.Complete(args.Sequence)

	for {
		cfg := ck.loadConfig(forceRefresh)
		forceRefresh = false
		if cfg == nil {
			forceRefresh = true
			time.Sleep(routeRetryPause)
			continue
		}

		_, servers, ok := cfg.GidServers(shard)
		if !ok || len(servers) == 0 {
			forceRefresh = true
			time.Sleep(routeRetryPause)
			continue
		}
		gid := cfg.Shards[shard]
		rck := ck.groupClerk(gid, servers)
		args.Ack = ck.requests.Ack()
		err := rck.PutRequest(args)
		switch err {
		case rpc.OK, rpc.ErrNoKey, rpc.ErrMaybe:
			return err
		case rpc.ErrVersion:
			return rpc.ErrVersion
		case rpc.ErrStaleRequest:
			panic("shardkv: active request was acknowledged before completion")
		case rpc.ErrWrongGroup, rpc.ErrWrongLeader:
			// 路由切换也必须保留同一次请求的 ClientID 和 Sequence。
			forceRefresh = true
			time.Sleep(routeRetryPause)
		}
	}
}
