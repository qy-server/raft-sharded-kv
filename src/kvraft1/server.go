package kvraft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"6.5840/kvraft1/dedup"
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me int

	mu       sync.Mutex
	rsm      *rsm.RSM
	values   map[string]valueEntry
	requests dedup.Table
}

type valueEntry struct {
	Value   string
	Version rpc.Tversion
}

// DoOp 是 KV 状态机执行已提交日志的唯一入口。RPC handler 只能把请求提交给
// RSM，不能绕过 Raft 直接读写 values，否则不同副本可能观察到不同顺序。
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		entry, exists := kv.values[args.Key]
		if !exists {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{
			Value:   entry.Value,
			Version: entry.Version,
			Err:     rpc.OK,
		}

	case rpc.PutArgs:
		return kv.requests.Do(args, func() rpc.PutReply { return kv.putLocked(args) })
	default:
		panic(fmt.Sprintf("kvraft: 不支持的操作类型 %T", req))
	}
}

func (kv *KVServer) putLocked(args rpc.PutArgs) rpc.PutReply {
	entry, exists := kv.values[args.Key]
	if !exists {
		// 新键只能从版本 0 开始创建，首次写入后的版本为 1。
		if args.Version != 0 {
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
		kv.values[args.Key] = valueEntry{
			Value:   args.Value,
			Version: 1,
		}
		return rpc.PutReply{Err: rpc.OK}
	}

	// 条件写处理并发冲突；同一次请求的重试已由去重表拦截。
	if entry.Version != args.Version {
		return rpc.PutReply{Err: rpc.ErrVersion}
	}
	entry.Value = args.Value
	entry.Version++
	kv.values[args.Key] = entry
	return rpc.PutReply{Err: rpc.OK}
}

// Snapshot 在同一把状态机锁下编码全部键值和版本号，确保得到的是某一个
// 已提交日志下标对应的完整状态，而不是并发修改过程中的混合视图。
func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	if err := encoder.Encode(kv.values); err != nil {
		panic(fmt.Sprintf("kvraft: 编码快照失败：%v", err))
	}
	if err := encoder.Encode(kv.requests); err != nil {
		panic(fmt.Sprintf("kvraft: 编码去重状态失败：%v", err))
	}
	return buffer.Bytes()
}

// Restore 先把快照解码到临时 map，确认数据完整后再原子替换当前状态。
// 空快照表示之前还没有生成过快照，此时保留 StartKVServer 创建的空数据库。
func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}

	values := make(map[string]valueEntry)
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&values); err != nil {
		panic(fmt.Sprintf("kvraft: 解码快照失败：%v", err))
	}
	if values == nil {
		values = make(map[string]valueEntry)
	}
	var requests dedup.Table
	// 旧快照只有键值数据，对应未携带请求身份的旧客户端。
	if err := decoder.Decode(&requests); err != nil && err != io.EOF {
		panic(fmt.Sprintf("kvraft: 解码去重状态失败：%v", err))
	}

	kv.mu.Lock()
	kv.values = values
	kv.requests = requests
	kv.mu.Unlock()
}

// Get 将读请求也提交到 Raft，从而让读操作和之前的写操作具有统一的全局顺序。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, result := kv.rsm.SubmitWithTimeout(*args, time.Second)
	if err != rpc.OK {
		*reply = rpc.GetReply{Err: err}
		return
	}
	resultReply, ok := result.(rpc.GetReply)
	if !ok {
		panic(fmt.Sprintf("kvraft: Get 收到错误的执行结果类型 %T", result))
	}
	*reply = resultReply
}

// Put 通过 RSM 等待条件写被 Raft 提交并执行，然后把确定结果返回给 Clerk。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.SubmitWithTimeout(*args, time.Second)
	if err != rpc.OK {
		*reply = rpc.PutReply{Err: err}
		return
	}
	resultReply, ok := result.(rpc.PutReply)
	if !ok {
		panic(fmt.Sprintf("kvraft: Put 收到错误的执行结果类型 %T", result))
	}
	*reply = resultReply
}

// StartKVServer 和 MakeRSM 必须快速返回，所有长期任务都应在 goroutine 中运行。
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 注册所有会放入 interface 并经过 RPC 或 Raft 日志编码的具体类型。
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:     me,
		values: make(map[string]valueEntry),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
