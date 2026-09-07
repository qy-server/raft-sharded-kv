package kvraft

import (
	"sync"
	"time"

	"6.5840/kvraft1/dedup"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

const clerkRetryPause = 10 * time.Millisecond

type Clerk struct {
	clnt     *tester.Clnt
	servers  []string
	mu       sync.Mutex
	leader   int
	requests dedup.Client
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	if len(servers) == 0 {
		panic("kvraft: Clerk 至少需要一台服务器")
	}
	return &Clerk{clnt: clnt, servers: append([]string(nil), servers...)}
}

func (ck *Clerk) Leader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) rememberLeader(server int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leader = server
}

func (ck *Clerk) advanceLeader(failedServer int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if ck.leader == failedServer {
		ck.leader = (failedServer + 1) % len(ck.servers)
	}
}

func (ck *Clerk) retryAfterFailure(server int, failedAttempts *int) {
	ck.advanceLeader(server)
	*failedAttempts++
	if *failedAttempts >= len(ck.servers) {
		*failedAttempts = 0
		time.Sleep(clerkRetryPause)
	}
}

// Get 也经过 Raft；失败后轮询 Leader，不读取未确认的本地状态。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	failedAttempts := 0
	for {
		server := ck.Leader()
		reply := rpc.GetReply{}
		if ck.clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply) {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:
				ck.rememberLeader(server)
				return reply.Value, reply.Version, reply.Err
			}
		}
		ck.retryAfterFailure(server, &failedAttempts)
	}
}

// Put 在所有重试中保留请求身份，服务端返回已提交的原始结果。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	args.ClientID, args.Sequence = ck.requests.Begin()
	defer ck.requests.Complete(args.Sequence)
	failedAttempts := 0
	for {
		args.Ack = ck.requests.Ack()
		server := ck.Leader()
		reply := rpc.PutReply{}
		if ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply) {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey, rpc.ErrVersion:
				ck.rememberLeader(server)
				return reply.Err
			case rpc.ErrStaleRequest:
				panic("kvraft: active request was acknowledged before completion")
			}
		}
		ck.retryAfterFailure(server, &failedAttempts)
	}
}
