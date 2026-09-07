package rsm

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Server、Epoch 和 Sequence 共同构成一次操作的全局唯一标识。
	// 所有字段都必须导出，否则 labgob 无法通过 RPC 或 Raft 日志编码它们。
	Server   int
	Epoch    uint64
	Sequence uint64
	Req      any
}

type opID struct {
	server   int
	epoch    uint64
	sequence uint64
}

type applyResult struct {
	reply any
}

// 需要通过 Raft 复制状态的服务（例如 ../server.go）必须实现 StateMachine。
// RSM 调用 DoOp 执行已经提交的业务操作，并通过 Snapshot/Restore 保存和恢复
// 状态机数据。快照只保存业务状态；waiters、epoch 等进程内控制信息不需要持久化。
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // Raft 持久化状态达到该大小时创建快照
	sm           StateMachine

	// epoch 在每次服务器启动时随机生成，避免重启后新请求与旧日志重放使用
	// 相同的 Sequence。nextSequence 只需在当前进程内单调递增。
	epoch        uint64
	nextSequence uint64

	// waiters 将操作身份映射到正在等待提交结果的 Submit 调用。
	waiters  map[opID]chan applyResult
	done     chan struct{}
	raftDone <-chan struct{}
}

// servers 包含所有 Raft 副本的 RPC 端点，它们共同组成容错状态机。
// me 是当前服务器在 servers 中的下标。
// KV 服务通过底层 Raft 保存快照。Raft 应当原子保存自己的持久化状态和状态机
// 快照。当 Raft 状态接近 maxraftstate 时，RSM 创建快照以便 Raft 回收旧日志；
// maxraftstate 为 -1 时不创建快照。
//
// MakeRSM 必须快速返回，因此消费 applyCh 等长期任务需要放入 goroutine。
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	var epochBytes [8]byte
	if _, err := cryptorand.Read(epochBytes[:]); err != nil {
		panic("rsm: 无法生成服务器启动标识")
	}

	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		epoch:        binary.LittleEndian.Uint64(epochBytes[:]),
		waiters:      make(map[opID]chan applyResult),
		done:         make(chan struct{}),
	}

	// 必须在启动 Raft 及其后台 goroutine 之前恢复状态机。Raft 的持久化状态已经
	// 丢弃了快照覆盖的旧日志，如果跳过这里，重启后的服务将永久缺少那部分数据。
	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if stoppable, ok := rsm.rf.(interface{ Done() <-chan struct{} }); ok {
		rsm.raftDone = stoppable.Done()
	}
	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit 将命令交给 Raft，并等待该命令提交和执行。如果当前服务器不是 Leader，
// 或等待期间失去 Leader 身份，则返回 ErrWrongLeader，让调用方寻找新 Leader。
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	return rsm.SubmitWithTimeout(req, 0)
}

// SubmitWithTimeout bounds one RPC attempt; a timeout does not cancel a Raft
// entry or imply that a write failed. Retries must keep their client identity.
// A zero timeout preserves the course's blocking Submit contract.
func (rsm *RSM) SubmitWithTimeout(req any, timeout time.Duration) (rpc.Err, any) {
	select {
	case <-rsm.done:
		return rpc.ErrWrongLeader, nil
	case <-rsm.raftDone:
		return rpc.ErrWrongLeader, nil
	default:
	}
	rsm.mu.Lock()
	rsm.nextSequence++
	op := Op{
		Server:   rsm.me,
		Epoch:    rsm.epoch,
		Sequence: rsm.nextSequence,
		Req:      req,
	}
	id := opID{server: op.Server, epoch: op.Epoch, sequence: op.Sequence}

	// 必须在调用 Start 之前注册等待者，否则日志可能先提交，导致通知丢失。
	waiter := make(chan applyResult, 1)
	rsm.waiters[id] = waiter
	rsm.mu.Unlock()
	defer rsm.removeWaiter(id, waiter)

	if rsm.rf == nil {
		return rpc.ErrWrongLeader, nil
	}

	_, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	var deadline <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		deadline = timer.C
	}

	// 定期检查任期，避免日志被覆盖后仍然等待。
	leadershipTicker := time.NewTicker(20 * time.Millisecond)
	defer leadershipTicker.Stop()

	for {
		select {
		case result := <-waiter:
			return rpc.OK, result.reply
		case <-deadline:
			select {
			case result := <-waiter:
				return rpc.OK, result.reply
			default:
				return rpc.ErrWrongLeader, nil
			}
		case <-rsm.raftDone:
			return rpc.ErrWrongLeader, nil
		case <-leadershipTicker.C:
			currentTerm, stillLeader := rsm.rf.GetState()
			if currentTerm != term || !stillLeader {
				// 提交结果和 Leader 变化可能同时发生，优先返回已经执行的结果。
				select {
				case result := <-waiter:
					return rpc.OK, result.reply
				default:
				}
				return rpc.ErrWrongLeader, nil
			}
		case <-rsm.done:
			return rpc.ErrWrongLeader, nil
		}
	}
}

func (rsm *RSM) removeWaiter(id opID, waiter chan applyResult) {
	rsm.mu.Lock()
	if rsm.waiters[id] == waiter {
		delete(rsm.waiters, id)
	}
	rsm.mu.Unlock()
}

// applier 是状态机执行和快照安装的唯一入口。它严格按照 Raft 的提交顺序
// 更新状态机，并只唤醒等待同一个操作身份的 Submit 调用。
func (rsm *RSM) applier() {
	defer close(rsm.done)

	for {
		var msg raftapi.ApplyMsg
		select {
		case <-rsm.raftDone:
			return
		case received, ok := <-rsm.applyCh:
			if !ok {
				return
			}
			msg = received
		}
		if msg.SnapshotValid {
			// InstallSnapshot 已经让 Raft 丢弃快照覆盖的日志；状态机必须同步
			// 切换到快照状态，随后才能继续执行 SnapshotIndex 之后的命令。
			if len(msg.Snapshot) > 0 {
				rsm.sm.Restore(msg.Snapshot)
			}
			continue
		}

		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			panic("rsm: Raft 提交了未知类型的命令")
		}
		reply := rsm.sm.DoOp(op.Req)
		id := opID{server: op.Server, epoch: op.Epoch, sequence: op.Sequence}

		// 快照必须在 DoOp 之后创建，这样快照内容和传给 Raft 的 CommandIndex表示完全相同的状态边界。
		if rsm.shouldSnapshot() {
			rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
		}

		rsm.mu.Lock()
		if waiter := rsm.waiters[id]; waiter != nil {
			// 缓冲区容量为 1，即使 Submit 正在检查任期，applier 也不会阻塞。
			select {
			case waiter <- applyResult{reply: reply}:
			default:
			}
		}
		rsm.mu.Unlock()
	}
}

// shouldSnapshot 判断持久化 Raft 状态是否已经达到日志压缩阈值。
// maxraftstate 为 -1 表示本次实验明确禁用快照。
func (rsm *RSM) shouldSnapshot() bool {
	return rsm.rf != nil &&
		rsm.maxraftstate >= 0 &&
		rsm.rf.PersistBytes() >= rsm.maxraftstate
}
