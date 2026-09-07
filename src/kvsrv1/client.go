package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

// Clerk 是 KV 服务的客户端代理。应用程序通过 Clerk 的 Get 和 Put 方法访问 KVServer。
type Clerk struct {
	clnt   *tester.Clnt
	server string
}

// MakeClerk 创建并初始化一个 Clerk。
// 返回值类型是 kvtest.IKVClerk 接口。
// *Clerk 实现了该接口要求的 Get 和 Put 方法
// 因此可以作为 kvtest.IKVClerk 返回。
func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get 获取指定 key 当前对应的 value 和 version
// 如果 key 不存在，返回 ErrNoKey
// 如果遇到除 ErrNoKey 以外的错误，例如请求丢失、响应丢失或服务器暂时不可达，Get 应该一直重试，直到得到明确结果
// 可以使用下面的代码发送 RPC:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
// args 和 reply 的类型必须与服务器端 RPC 处理函数声明的
// 参数类型完全匹配，包括参数是否为指针
// 此外，reply 必须以指针形式传入，因为 RPC 框架需要把服务器返回的数据写入 reply

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// 你需要修改并实现这个函数。
	// 主要步骤：
	//1. 根据 key 构造 rpc.GetArgs；
	//2. 创建 rpc.GetReply；
	//3. 调用 KVServer.Get；
	//4. 如果 RPC 调用失败，则继续重试；
	//5. 如果服务器返回 OK，则返回 value 和 version；
	//6. 如果服务器返回 ErrNoKey，则返回 ErrNoKey。
	args := rpc.GetArgs{
		Key: key,
	}
	for {
		// 每次重试都创建一个新的 reply，
		// 避免上一次 RPC 留下的数据影响本次结果。
		var reply rpc.GetReply
		ok := ck.clnt.Call(
			ck.server,
			"KVServer.Get",
			&args,
			&reply,
		)
		// RPC 调用失败，可能是请求丢失、回复丢失或网络故障。
		// 按照实验要求继续重试。
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// RPC 成功到达并收到了服务器回复。
		switch reply.Err {
		case rpc.OK:
			// key 存在，返回对应的值、版本号和 OK。
			return reply.Value, reply.Version, rpc.OK

		case rpc.ErrNoKey:
			// key 不存在，这是明确结果，不需要继续重试。
			return "", 0, rpc.ErrNoKey

		default:
			// 注释要求：除 ErrNoKey 之外的其他错误一直重试。
			continue
		}
	}
	//return "", 0, rpc.ErrNoKey
}

// Put 只有在请求中的 version 与服务器上该 key 当前的version 相同时，才会把 key 更新为新的 value。
// 如果两个版本号不匹配，服务器应该返回 ErrVersion。
// 如果 Put 的第一次 RPC 请求明确收到了 ErrVersion，Put 应该向应用程序返回 ErrVersion。
// 因为这是第一次请求，并且服务器明确返回版本不匹配，所以可以确定该 Put 没有在服务器上执行。
// 如果某次 RPC 没有收到响应，Clerk 会重新发送相同请求。
// 如果服务器对重发的 RPC 返回 ErrVersion，Put 必须向应用程序返回 ErrMaybe。
// 原因是之前的 RPC 可能已经被服务器成功处理，只是服务器的响应在网络中丢失了。此时 Clerk 不知道之前的 Put 到底有没有执行成功，因此只能返回“可能成功”的 ErrMaybe。
// 可以使用下面的代码发送 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// args 和 reply 的类型必须与服务器端 RPC 处理函数声明的参数类型完全匹配，包括参数是否为指针。
// 此外，reply 必须以指针形式传入。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// 你需要修改并实现这个函数。
	//主要步骤：
	//1. 根据 key、value 和 version 构造 rpc.PutArgs；
	//2. 创建 rpc.PutReply；
	//3. 调用 KVServer.Put；
	//4. 如果 RPC 调用失败，则记录已经发生重发并继续重试；
	//5. 如果返回 OK，则返回 OK；
	//6. 如果第一次请求返回 ErrVersion，则返回 ErrVersion；
	//7. 如果重发请求返回 ErrVersion，则返回 ErrMaybe；
	//8. 如果返回 ErrNoKey，则返回 ErrNoKey。
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	// 表示当前请求是否已经因为 RPC 失败而重发过。
	resent := false
	for {
		var reply rpc.PutReply
		ok := ck.clnt.Call(
			ck.server,
			"KVServer.Put",
			&args,
			&reply,
		)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			resent = true
			continue
		}
		switch reply.Err {
		case rpc.OK:
			return rpc.OK
		case rpc.ErrNoKey:
			return rpc.ErrNoKey

		case rpc.ErrVersion:
			if resent {
				// 之前的请求可能已经成功执行，
				// 只是成功回复丢失，因此无法确定。
				return rpc.ErrMaybe
			}

			// 第一次请求就明确返回版本不匹配，
			// 可以确定服务器没有执行此次 Put。
			return rpc.ErrVersion

		default:
			// 当前实验通常不会出现其他错误。
			// 为了满足持续重试的要求，这里继续发送请求。
			resent = true
			continue
		}

	}
	//return rpc.ErrNoKey
}
