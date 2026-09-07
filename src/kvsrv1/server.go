package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// entry 表示服务器中的一条键值记录。
// 每条记录同时保存值和版本号。
type entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	// mu 保护 data，避免多个 RPC 并发访问 map。
	mu sync.Mutex

	// data 保存 key 对应的值和版本号。
	data map[string]entry
}

// MakeKVServer 创建并初始化 KVServer。
func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.data = make(map[string]entry)
	return kv
}

// Get 返回 args.Key 对应的 value 和 version。
// 如果 key 不存在，则返回 ErrNoKey。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	item, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = item.Value
	reply.Version = item.Version
	reply.Err = rpc.OK
}

// 如果 args.Version 与服务器中 key 当前的版本号相同，
// 就更新这个 key 的 value。
//
// 如果版本号不同，则返回 ErrVersion。
//
// 如果 key 不存在：
//   - args.Version 等于 0 时，创建该 key；
//   - args.Version 不等于 0 时，返回 ErrNoKey。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	item, exists := kv.data[args.Key]

	// key 不存在。
	if !exists {
		// Version 为 0，表示客户端认为这个 key 当前不存在，
		// 因此允许创建。
		if args.Version == 0 {
			kv.data[args.Key] = entry{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
			return
		}

		// key 不存在，但客户端传入了非零版本。
		reply.Err = rpc.ErrNoKey
		return
	}

	// key 存在，但版本号不匹配。
	// 失败 Put 的线性化点：
	// 在锁内原子观察到版本不匹配。
	if item.Version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	// key 存在且版本号匹配，更新值并增加版本号。
	// 失败 Put 的线性化点：
	// 在锁内原子观察到版本不匹配。
	item.Value = args.Value
	item.Version++

	kv.data[args.Key] = item
	reply.Err = rpc.OK
}

// 这些参数是为后续复制式 KVServer 实验准备的，
// 当前实验中可以忽略。
func StartKVServer(
	tc *tester.TesterClnt,
	ends []*labrpc.ClientEnd,
	gid tester.Tgid,
	srv int,
	persister *tester.Persister,
) []any {
	kv := MakeKVServer()
	return []any{kv}
}
