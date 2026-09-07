package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk 是键值存储客户端 Clerk 的 Go 接口。
	// 该接口隐藏了 ck 所使用的具体 Clerk 类型，
	// 但保证 ck 支持 Put 和 Get 方法。
	// 测试程序在调用 MakeLock() 时会传入这个 Clerk。
	ck kvtest.IKVClerk
	// 你可以在这里添加字段。
	key string
	id  string
}

// 测试程序会调用 MakeLock()，并传入一个键值存储 Clerk。
// 你的代码可以通过调用 lk.ck.Put() 或 lk.ck.Get()
// 来执行 Put 或 Get 操作。
//
// 该接口通过 lockname 参数支持创建多把锁。
// 不同名称的锁应该彼此独立、互不影响。
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:  ck,
		key: lockname,
		id:  kvtest.RandValue(8), //生成一个长度为 8 的随机字符串
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.key)

		if err == rpc.ErrNoKey {
			// key 不存在，尝试创建并持有锁。
			err = lk.ck.Put(lk.key, lk.id, 0)

			switch err {
			case rpc.OK:
				return

			case rpc.ErrMaybe:
				// Put 可能已经成功，重新读取进行确认。
				current, _, getErr := lk.ck.Get(lk.key)
				if getErr == rpc.OK && current == lk.id {
					return
				}

				// 没看到自己的 ID，重新竞争。
				continue

			default:
				// ErrVersion 表示其他客户端抢先创建；
				// 其他错误也重新读取状态。
				continue
			}
		}

		if err != rpc.OK {
			continue
		}

		// 锁被某个客户端占用。
		if value != "" {
			continue
		}

		// 锁空闲，用刚刚读到的版本进行条件 Put。
		err = lk.ck.Put(lk.key, lk.id, version)

		switch err {
		case rpc.OK:
			return

		case rpc.ErrMaybe:
			// Put 可能成功，检查当前持有者。
			current, _, getErr := lk.ck.Get(lk.key)
			if getErr == rpc.OK && current == lk.id {
				return
			}

			// 当前状态不是自己持有，重新竞争。
			continue

		case rpc.ErrVersion:
			// 其他客户端抢先更新，重新读取。
			continue

		default:
			continue
		}
	}
}

func (lk *Lock) Release() {
	// 读取锁当前的持有者和版本号；
	//判断锁是否由自己持有；
	//使用刚读到的版本号，条件 Put 空字符串；
	//如果版本号冲突，重新 Get，不能继续使用旧版本。
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			// 锁对应的 key 不存在，说明当前没有可释放的锁
			return
		}
		if err != rpc.OK {
			// 读取失败，重新尝试
			continue
		}

		// 当前锁不是自己持有的，不能释放
		if value != lk.id {
			return
		}
		// 将锁的 value 清空，表示锁变为空闲状态
		err = lk.ck.Put(lk.key, "", version)

		if err == rpc.OK {
			// 成功释放锁
			return
		}
		if err == rpc.ErrVersion {
			// Get 和 Put 之间，锁状态发生了变化
			// 旧 version 已经过期，重新 Get
			continue
		}
		// 其他错误，重新执行整个释放流程
		continue
	}
}
