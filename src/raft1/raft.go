package raft

// 文件 ../raftapi/raftapi.go 定义了 Raft 必须向服务器（或测试程序）
// 暴露的接口；各个函数的更多细节请参阅下方对应的注释。
//
// 此外，Make() 会创建一个实现 Raft 接口的新节点。

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// Raft 表示一个 Raft 节点的 Go 对象。
const (
	Follower = iota
	Candidate
	Leader
)

const (
	persistMagic      = "RFT2"
	persistHeaderSize = len(persistMagic) + 5*8
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu          sync.Mutex          // 保护对当前节点共享状态的并发访问
	peers       []*labrpc.ClientEnd // 所有 Raft 节点的 RPC 端点
	persister   *tester.Persister   // 保存当前节点持久化状态的对象
	encodedLogs [][]byte            // 与 logs 一一对应的编码缓存
	me          int                 // 当前节点在 peers[] 中的下标
	dead        int32
	done        chan struct{}
	stopped     chan struct{}
	workers     sync.WaitGroup
	currentTerm int        //当前任期
	votedFor    int        //当前任期票投给谁
	logs        []LogEntry //日志状态
	nextIndex   []int      //下一次准备发给某 Follower 的日志 index
	matchIndex  []int      //已确认复制到某 Follower 的最大 index

	state            int //当前角色
	electionDeadline time.Time

	commitIndex int // 已知已经提交的最高日志下标
	lastApplied int // 已经发送给状态机的最高日志下标

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond

	replicateNotify []chan struct{} //通知对应 Follower 的复制 worker
	replicateBusy   []bool          //对应worker是否正在等待RPC，受rf.mu保护
	lastRescueSent  []time.Time     //上一次绕过阻塞 worker 发起追赶复制的时间
	//快照需要持久化
	lastIncludedIndex int //快照中的最后index
	lastIncludedTerm  int
	snapshot          []byte
	pendingSnapshot   *raftapi.ApplyMsg
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int        //新日志前一条日志的 index
	PrevLogTerm  int        //前一条日志的 term
	Entries      []LogEntry //准备复制的日志，空切片就是 heartbeat
	LeaderCommit int        //Leader 已知的 commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Stopped bool

	ConflictTerm  int //Follower 在 PrevLogIndex 这个位置上，实际保存的日志属于哪个 term
	ConflictIndex int //Leader 下一次可以直接尝试跳到的关键日志位置
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term    int
	Stopped bool
}

type RequestVoteArgs struct {
	// 在此处添加 3A、3B 所需的数据。
	Term        int // Candidate 当前任期
	CandidateId int // Candidate 是哪个服务器

	// 3B 日志复制阶段会真正用到
	LastLogIndex int // Candidate 最后一条日志的 index
	LastLogTerm  int // Candidate 最后一条日志的 term
}

type RequestVoteReply struct {
	// 在此处添加 3A 所需的数据。
	Term        int  // 接收方当前 term
	VoteGranted bool // 是否投票给 Candidate
}

func (rf *Raft) Kill() {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	atomic.StoreInt32(&rf.dead, 1)
	rf.state = Follower
	close(rf.done)
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
	go func() {
		rf.workers.Wait()
		close(rf.stopped)
	}()
}

// Done signals shutdown immediately; Stopped also waits for in-flight RPCs
// and every Raft-owned worker to finish. The caller owns applyCh.
func (rf *Raft) Done() <-chan struct{}    { return rf.done }
func (rf *Raft) Stopped() <-chan struct{} { return rf.stopped }

func (rf *Raft) launch(fn func()) {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.workers.Add(1)
	rf.mu.Unlock()
	go func() {
		defer rf.workers.Done()
		fn()
	}()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// 日志辅助函数 返回当前 Raft 节点最后一条日志的下标
// 建立统一的索引辅助层
// 当前 rf.logs 中保存的第一个 Raft 全局日志下标是多少
func (rf *Raft) firstLogIndexLocked() int {
	return rf.lastIncludedIndex
}

// 返回绝对末尾下标
func (rf *Raft) lastLogIndexLocked() int {
	return rf.lastIncludedIndex + len(rf.logs) - 1
}

// Raft全局下标
func (rf *Raft) toOffsetLocked(index int) int {
	return index - rf.lastIncludedIndex
}

// 判断某个 Raft 全局日志 index 当前是否还存在于 rf.logs 中
func (rf *Raft) containsIndexLocked(index int) bool {
	return index >= rf.lastIncludedIndex &&
		index <= rf.lastLogIndexLocked()
}

// 根据 Raft 全局 index 查对应日志的 term
func (rf *Raft) termAtLocked(index int) (int, bool) {
	if !rf.containsIndexLocked(index) {
		return 0, false
	}

	return rf.logs[rf.toOffsetLocked(index)].Term, true
}

// 负责时间的抽象转换函数
func (rf *Raft) resetElectionDeadlineLocked() {

	rf.electionDeadline = time.Now().Add(randomElectionTimeout())
}

// 负责状态的抽象转换函数1
func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		// 3C 实现后需要持久化
		rf.persist()
	}
	rf.state = Follower
}

// 负责状态的抽象转换函数2
func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader
	lastIndex := rf.lastLogIndexLocked()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
		if len(rf.lastRescueSent) == len(rf.peers) {
			rf.lastRescueSent[i] = time.Time{}
		}
	}
	// Leader 肯定拥有自己的全部日志
	rf.matchIndex[rf.me] = lastIndex
	rf.nextIndex[rf.me] = lastIndex + 1
}

// 辅助函数：返回leader冲突term的最后index，如果leader没有冲突term返回-1
func (rf *Raft) lastIndexOfTermLocked(term int) int {
	for offset := len(rf.logs) - 1; offset >= 0; offset-- {
		if rf.logs[offset].Term == term {
			return rf.lastIncludedIndex + offset
		}
	}
	return -1
}

// 辅助函数：把已经提交、但还没有真正交给状态机的日志，按照顺序发送到 applyCh
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.killed() && rf.pendingSnapshot == nil &&
			rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.pendingSnapshot != nil {
			message := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			// Snapshot 已经包含了 SnapshotIndex 之前的状态，
			// 所以 lastApplied 至少推进到 SnapshotIndex。
			if rf.lastApplied < message.SnapshotIndex {
				rf.lastApplied = message.SnapshotIndex
			}
			rf.mu.Unlock()
			// 锁外发送
			select {
			case rf.applyCh <- message:
			case <-rf.done:
				return
			}
			continue
		}
		start := rf.lastApplied + 1
		if start <= rf.lastIncludedIndex {
			start = rf.lastIncludedIndex + 1
		}
		end := rf.commitIndex
		messages := make(
			[]raftapi.ApplyMsg,
			0,
			end-start+1,
		)
		for index := start; index <= end; index++ {
			messages = append(messages, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.toOffsetLocked(index)].Command,
				CommandIndex: index,
			})
		}
		// 这批日志由唯一 applier 认领
		rf.lastApplied = end
		rf.mu.Unlock()
		// 必须锁外发送
		for _, message := range messages {
			select {
			case rf.applyCh <- message:
			case <-rf.done:
				return
			}
		}
	}
}

// Follower 的 InstallSnapshot handler
func (rf *Raft) InstallSnapshot(
	args *InstallSnapshotArgs,
	reply *InstallSnapshotReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	reply.Stopped = rf.killed()
	if reply.Stopped {
		return
	}
	if args.Term < rf.currentTerm {
		return
	}

	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionDeadlineLocked()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	// 安装逻辑
	// 1. 先判断旧日志中是否存在相同的 snapshot 边界
	var suffix []LogEntry
	localTerm, ok := rf.termAtLocked(args.LastIncludedIndex)
	if ok && localTerm == args.LastIncludedTerm {
		// 本地和 snapshot 在边界处完全一致
		offset := rf.toOffsetLocked(args.LastIncludedIndex)
		// 保留边界之后的真实日志
		suffix = append(
			[]LogEntry(nil),
			rf.logs[offset+1:]...,
		)
	}
	// 2. 创建新的日志数组
	// logs[0] 将作为新的 snapshot dummy
	newLogs := make([]LogEntry, 1, len(suffix)+1)
	newLogs[0] = LogEntry{
		Term:    args.LastIncludedTerm,
		Command: nil,
	}
	// 3. 如果边界一致，就把旧 suffix 接回来
	newLogs = append(newLogs, suffix...)
	// 4. 更新 snapshot 边界
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// 5. 替换旧日志
	rf.replaceLogs(newLogs)
	rf.lastIncludedIndex =
		args.LastIncludedIndex
	rf.lastIncludedTerm =
		args.LastIncludedTerm
	rf.snapshot = append(
		[]byte(nil),
		args.Data...,
	)
	rf.commitIndex =
		args.LastIncludedIndex
	rf.lastApplied =
		args.LastIncludedIndex
	//必须在 RPC 返回前执行持久化
	rf.persist()
	//挂起ApplyMsg
	rf.pendingSnapshot = &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot: append(
			[]byte(nil),
			rf.snapshot...,
		),
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.applyCond.Signal()
}

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 默认返回当前 term，默认失败
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0
	reply.Stopped = rf.killed()
	if reply.Stopped {
		return
	}
	// Leader 的 term 太旧，拒绝
	if args.Term < rf.currentTerm {
		return
	}
	// 接受合法 Leader
	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionDeadlineLocked()
	reply.Term = rf.currentTerm
	// 1. 检查 PrevLogIndex 是否存在
	// Leader 要检查的位置比 Follower 最后一条日志还靠后
	if args.PrevLogIndex > rf.lastLogIndexLocked() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndexLocked() + 1
		return
	}
	// Snapshot 之后，PrevLogIndex 可能已经被压缩掉。
	// 当前第一轮 lastIncludedIndex == 0 时，基本不会进入这里。
	if args.PrevLogIndex < rf.firstLogIndexLocked() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.firstLogIndexLocked() + 1
		return
	}
	// 2. 检查 PrevLogTerm
	prevLogTerm, ok := rf.termAtLocked(args.PrevLogIndex)
	if !ok {
		return
	}
	if prevLogTerm != args.PrevLogTerm {
		// PrevLogIndex 存在，但是 term 不一样
		conflictTerm := prevLogTerm
		conflictIndex := args.PrevLogIndex
		// 向前寻找 conflictTerm 第一次出现的位置
		// conflictIndex 是绝对 index，所以不能再写 rf.logs[conflictIndex-1]
		for conflictIndex > rf.firstLogIndexLocked() {
			previousTerm, ok :=
				rf.termAtLocked(conflictIndex - 1)

			if !ok || previousTerm != conflictTerm {
				break
			}
			conflictIndex--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = conflictIndex
		return
	}
	// 3. PrevLog 匹配成功，开始合并日志
	// insertIndex 是绝对 Raft index
	insertIndex := args.PrevLogIndex + 1
	changed := false
	for i, entry := range args.Entries {
		// localIndex 仍然是绝对 Raft index
		localIndex := insertIndex + i

		// Follower 当前已经有这个 index
		if localIndex <= rf.lastLogIndexLocked() {
			localTerm, ok := rf.termAtLocked(localIndex)

			if !ok {
				// 理论上前面的范围检查保证这里应该存在
				continue
			}
			// 同一个 index，但是 term 不同
			if localTerm != entry.Term {
				// localIndex 是绝对 index
				// 先转换成 slice offset
				offset := rf.toOffsetLocked(localIndex)
				// 删除冲突日志以及它后面的所有日志
				rf.truncateLogs(offset)
				// 把 Leader 剩余日志追加过来
				rf.appendLogs(args.Entries[i:]...)
				changed = true
				break
			}
			// term 相同，说明这条日志已经一致
			// 继续检查下一条
		} else {
			// Follower 日志比较短，
			// 直接追加 Leader 剩余日志
			rf.appendLogs(args.Entries[i:]...)
			changed = true
			break
		}
	}
	if changed {
		rf.persist()
	}
	// 4. 推进 commitIndex
	// 本次 RPC 能确认匹配到的最后一个绝对 index
	lastNewIndex :=
		args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Signal()
		}
	}
	reply.Success = true
}

// 辅助函数：Leader 根据多数派推进 commitIndex
func (rf *Raft) advanceCommitIndexLocked() bool {
	if rf.state != Leader {
		return false
	}
	majority := len(rf.peers)/2 + 1
	for n := rf.lastLogIndexLocked(); n > rf.commitIndex; n-- {
		// Raft 的关键安全限制
		entryTerm, ok := rf.termAtLocked(n)
		if !ok || entryTerm != rf.currentTerm {
			continue
		}
		replicated := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= n {
				replicated++
			}
		}
		if replicated >= majority {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return true
		}
	}
	return false
}

// Lead用来给某个Follower复制日志的函数
func (rf *Raft) replicateUntilStable(peer int) {
	for {
		// 1. 锁内构造请求
		rf.mu.Lock()
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		next := rf.nextIndex[peer]
		if next <= rf.lastIncludedIndex {
			// 发送 InstallSnapshot
			args := InstallSnapshotArgs{
				Term:              term,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data: append(
					[]byte(nil),
					rf.snapshot...,
				),
			}
			rf.mu.Unlock()
			var reply InstallSnapshotReply

			ok := rf.sendInstallSnapshot(
				peer,
				&args,
				&reply,
			)

			if !ok {
				// 网络失败：不修改 nextIndex 不修改 matchIndex 等下一次 heartbeat / notify 重试
				return
			}
			rf.mu.Lock()
			if rf.killed() || reply.Stopped {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.resetElectionDeadlineLocked()
				rf.mu.Unlock()
				return
			}
			// 这个 RPC 已经过期
			if rf.state != Leader ||
				rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			// 单调更新 replication 状态
			if args.LastIncludedIndex > rf.matchIndex[peer] {
				rf.matchIndex[peer] = args.LastIncludedIndex
			}

			if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
				rf.nextIndex[peer] = args.LastIncludedIndex + 1
			}

			rf.mu.Unlock()
			continue
		}

		prevLogIndex := next - 1
		prevLogTerm, ok := rf.termAtLocked(prevLogIndex)
		if !ok {
			rf.mu.Unlock()
			return
		}
		offset := rf.toOffsetLocked(next)
		entries := append(
			[]LogEntry(nil),
			rf.logs[offset:]...,
		)
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		// 2. 锁外发送
		var reply AppendEntriesReply
		if !rf.sendAppendEntries(peer, &args, &reply) {
			// 网络失败不是日志冲突。
			// 等待下一个 heartbeat/Start 通知。
			return
		}
		// 3. 锁内处理回复
		rf.mu.Lock()
		if rf.killed() || reply.Stopped {
			rf.mu.Unlock()
			return
		}
		// 更高任期永远优先处理
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			rf.resetElectionDeadlineLocked()
			rf.mu.Unlock()
			return
		}
		// 请求发出后，自己可能已经不是该 term 的 Leader
		if rf.state != Leader ||
			rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		//RPC成功
		if reply.Success {
			replicatedThrough :=
				args.PrevLogIndex + len(args.Entries)
			oldMatch := rf.matchIndex[peer]
			// matchIndex 只能向前移动
			if replicatedThrough > rf.matchIndex[peer] {
				rf.matchIndex[peer] = replicatedThrough
			}
			// nextIndex 也只能向前推进
			if replicatedThrough+1 > rf.nextIndex[peer] {
				rf.nextIndex[peer] = replicatedThrough + 1
			}
			// 判断这次是否真的复制了新的日志
			progressed :=
				rf.matchIndex[peer] > oldMatch
			// 尝试推进 commitIndex
			commitAdvanced := false
			if progressed {
				commitAdvanced =
					rf.advanceCommitIndexLocked()
			}
			// 判断这个 Follower 是否已经追平
			caughtUp :=
				rf.nextIndex[peer] >
					rf.lastLogIndexLocked()
			rf.mu.Unlock()
			// commitIndex 更新后，
			// 立即通知其他 Follower
			if commitAdvanced {
				rf.broadcastAppendEntries()
			}
			// 已经追平，不需要继续发送
			if caughtUp {
				return
			}
			// RPC 等待期间可能出现了新的 Start()
			// 所以还没追平就继续下一轮
			continue
		}
		// RPC 成功，但是日志不匹配
		// 本次 RPC 发出时使用的 nextIndex
		sentNext := args.PrevLogIndex + 1
		// 防止旧失败回复覆盖新的复制进度
		if rf.nextIndex[peer] != sentNext {
			rf.mu.Unlock()
			return
		}
		// 默认按照 Follower 给出的 ConflictIndex 回退
		newNext := reply.ConflictIndex
		// ConflictTerm 快速回退
		if reply.ConflictTerm != -1 {
			// Leader 查找自己日志中
			// ConflictTerm 最后一次出现的位置
			lastIndex :=
				rf.lastIndexOfTermLocked(
					reply.ConflictTerm,
				)
			if lastIndex >= 0 {
				// Leader 也有这个 term
				// 直接跳到这个 term 最后一条日志之后
				newNext = lastIndex + 1
			}
		}
		//nextIndex 边界保护
		if newNext < 1 {
			newNext = 1
		}
		// nextIndex 最大只能是：
		// lastLogIndex + 1
		maxNext :=
			rf.lastLogIndexLocked() + 1
		if newNext > maxNext {
			newNext = maxNext
		}
		// 不能回退到已经确认匹配的位置之前
		minNext :=
			rf.matchIndex[peer] + 1
		if newNext < minNext {
			newNext = minNext
		}
		// 真正更新 nextIndex
		rf.nextIndex[peer] = newNext
		rf.mu.Unlock()
		//  不创建新的 goroutine
		//  当前 worker 自己立即重新尝试
		//  发现冲突
		//     ↓
		//  修改 nextIndex
		//     ↓
		//  当前 worker 回到 for 顶部
		//     ↓
		//  重新构造 AppendEntries
		//     ↓
		//  重新发送
		continue
	}
}

// 返回 currentTerm，以及当前服务器是否认为自己是 Leader。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// 在此处编写 3A 的代码。
	term = rf.currentTerm
	isleader = !rf.killed() && rf.state == Leader
	return term, isleader
}

// encodeLogEntry 只在日志条目第一次出现时执行 GOB 编码。之后的 persist
// 直接复用结果，避免日志增长时反复反射编码全部历史命令。
func encodeLogEntry(entry LogEntry) []byte {
	var buffer bytes.Buffer
	if err := labgob.NewEncoder(&buffer).Encode(entry); err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func (rf *Raft) rebuildEncodedLogs() {
	rf.encodedLogs = make([][]byte, len(rf.logs))
	for i, entry := range rf.logs {
		rf.encodedLogs[i] = encodeLogEntry(entry)
	}
}

func (rf *Raft) appendLogs(entries ...LogEntry) {
	rf.logs = append(rf.logs, entries...)
	for _, entry := range entries {
		rf.encodedLogs = append(rf.encodedLogs, encodeLogEntry(entry))
	}
}

func (rf *Raft) truncateLogs(length int) {
	rf.logs = rf.logs[:length]
	rf.encodedLogs = rf.encodedLogs[:length]
}

func (rf *Raft) replaceLogs(logs []LogEntry) {
	rf.logs = logs
	rf.rebuildEncodedLogs()
}

// 将 Raft 的持久化状态保存到稳定存储中，以便节点崩溃并重启后恢复。
// 元数据使用固定宽度编码，每条日志保存预先生成的 GOB 字节；持久化内容仍然
// 包含完整日志，并通过 Persister.Save 与快照原子替换。
func (rf *Raft) persist() {
	if len(rf.encodedLogs) != len(rf.logs) {
		panic("raft: 日志与持久化编码缓存不一致")
	}

	totalSize := persistHeaderSize
	for _, encoded := range rf.encodedLogs {
		totalSize += 8 + len(encoded)
	}
	raftState := make([]byte, totalSize)
	copy(raftState, persistMagic)
	offset := len(persistMagic)
	putInt := func(value int) {
		binary.LittleEndian.PutUint64(raftState[offset:offset+8], uint64(int64(value)))
		offset += 8
	}
	putInt(rf.currentTerm)
	putInt(rf.votedFor)
	putInt(rf.lastIncludedIndex)
	putInt(rf.lastIncludedTerm)
	binary.LittleEndian.PutUint64(raftState[offset:offset+8], uint64(len(rf.encodedLogs)))
	offset += 8
	for _, encoded := range rf.encodedLogs {
		binary.LittleEndian.PutUint64(raftState[offset:offset+8], uint64(len(encoded)))
		offset += 8
		copy(raftState[offset:offset+len(encoded)], encoded)
		offset += len(encoded)
	}
	rf.persister.Save(raftState, rf.snapshot)
}

// readCachedPersist 解码按条目缓存的新格式，并保留原始条目字节供后续
// persist 复用。返回 false 表示数据是旧格式，应交给兼容解码器。
func (rf *Raft) readCachedPersist(data []byte) bool {
	if len(data) < persistHeaderSize || string(data[:len(persistMagic)]) != persistMagic {
		return false
	}

	offset := len(persistMagic)
	readInt := func() int {
		value := int(int64(binary.LittleEndian.Uint64(data[offset : offset+8])))
		offset += 8
		return value
	}
	currentTerm := readInt()
	votedFor := readInt()
	lastIncludedIndex := readInt()
	lastIncludedTerm := readInt()
	count := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	if count == 0 || count > uint64((len(data)-offset)/8) {
		panic("raft: 持久化日志数量无效")
	}

	logs := make([]LogEntry, int(count))
	encodedLogs := make([][]byte, int(count))
	for i := range logs {
		if offset+8 > len(data) {
			panic("raft: 持久化日志长度缺失")
		}
		length := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		if length > uint64(len(data)-offset) {
			panic("raft: 持久化日志内容不完整")
		}
		encoded := append([]byte(nil), data[offset:offset+int(length)]...)
		offset += int(length)
		if err := labgob.NewDecoder(bytes.NewReader(encoded)).Decode(&logs[i]); err != nil {
			panic(err)
		}
		encodedLogs[i] = encoded
	}
	if offset != len(data) {
		panic("raft: 持久化状态包含多余数据")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.encodedLogs = encodedLogs
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	return true
}

// 恢复此前持久化的状态。兼容优化前按整段 GOB 编码的旧格式。
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		// 第一次启动，没有旧状态。
		return
	}
	if rf.readCachedPersist(data) {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if err := d.Decode(&currentTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&logs); err != nil {
		panic(err)
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		panic(err)
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		panic(err)
	}
	if len(logs) == 0 {
		panic("raft: persisted log is empty")
	}
	// 全部解码成功后，再一次性替换内存状态。
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.rebuildEncodedLogs()
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// 返回 Raft 持久化状态占用的字节数。
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 上层服务通知 Raft：它已经创建了一个包含 index 及之前全部信息的快照。
// 这意味着上层服务不再需要 index 及之前的日志，Raft 此时应尽可能裁剪日志。
func (rf *Raft) Snapshot(index int, data []byte) {
	// 在此处编写 3D 的代码。
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	// 重复或更旧的快照
	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastApplied ||
		index > rf.lastLogIndexLocked() {
		return
	}
	oldBase := rf.lastIncludedIndex
	cut := index - oldBase

	boundaryTerm := rf.logs[cut].Term

	// 必须创建新底层数组，让旧日志可以被 GC
	tail := append(
		[]LogEntry(nil),
		rf.logs[cut+1:]...,
	)

	newLogs := make(
		[]LogEntry,
		1,
		len(tail)+1,
	)

	newLogs[0] = LogEntry{
		Term:    boundaryTerm,
		Command: nil,
	}

	newLogs = append(newLogs, tail...)

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = boundaryTerm
	rf.replaceLogs(newLogs)

	// 防止调用方以后修改原 byte slice
	rf.snapshot = append([]byte(nil), data...)

	// 原子保存元数据、尾日志和快照
	rf.persist()
}

// RequestVote RPC 处理函数示例。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 在此处编写 3A、3B 的代码。
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//获得自己的日志末尾
	myLastLogIndex := rf.lastLogIndexLocked()
	myLastLogTerm := rf.lastLogTermLocked()

	// 默认拒绝投票
	reply.VoteGranted = false
	if rf.killed() {
		reply.Term = rf.currentTerm
		return
	}

	// 1. Candidate 的 term 比我的旧，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// 2. Candidate 的 term 比我的新
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 3. 当前 term 一致
	// 如果我还没投票，或者已经投给这个 Candidate
	canVote := rf.votedFor == -1 ||
		rf.votedFor == args.CandidateId
	//判断候选人的日志是否至少和自己一样新
	upToDate :=
		args.LastLogTerm > myLastLogTerm ||
			(args.LastLogTerm == myLastLogTerm &&
				args.LastLogIndex >= myLastLogIndex)
	if canVote && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionDeadlineLocked()

		// 3C 实现后持久化 votedFor
		rf.persist()
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) lastLogTermLocked() int {
	return rf.logs[len(rf.logs)-1].Term
}

// 向某个服务器发送 RequestVote RPC 的示例代码。
// server 是目标服务器在 rf.peers[] 中的下标。
// args 中保存要发送的 RPC 请求参数。
// reply 用来接收 RPC 返回的数据，所以调用者应该传入 &reply，也就是 reply 的地址。
// 传给 Call() 的 args 和 reply 的类型，必须和 RPC 处理函数中声明的参数类型完全一致，包括它们是不是指针。

// labrpc 包模拟的是一个不可靠网络。
// 在这个网络中，服务器可能无法访问，
// 请求可能丢失，回复也可能丢失。

// Call() 会发送一个请求，然后等待服务器返回回复。
// 如果在超时时间内收到了回复，Call() 返回 true；否则 Call() 返回 false。
// 因此，Call() 有可能会等待一段时间之后才返回。
// Call() 返回 false，可能有多种原因：
// 1. 目标服务器已经宕机；
// 2. 目标服务器还活着，但是当前网络无法访问它；
// 3. RPC 请求在网络中丢失；
// 4. RPC 请求已经到达服务器并执行，但是返回的 reply 在网络中丢失。

// Call() 保证最终一定会返回，虽然可能会有一定延迟。
// 唯一的例外是：服务器端的 RPC handler 函数自己一直不返回。
// 因此，你不需要在 Call() 外面再自己实现一套超时机制。
// 如果想了解更多细节，可以查看 ../labrpc/labrpc.go 中的注释。

// 如果你的 RPC 一直无法正常工作，可以重点检查两件事：
//  1. RPC 参数结构体中的字段名是否以大写字母开头。
//     例如应该写：Term int
//     而不能写：term int
//     因为只有大写开头的字段才能被 RPC 正常序列化和传输。
//  2. 调用 RPC 时，reply 是否传入了地址 &reply，而不是直接传 reply 本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call(
		"Raft.AppendEntries",
		args,
		reply,
	)
	return ok
}

func (rf *Raft) sendInstallSnapshot(
	server int,
	args *InstallSnapshotArgs,
	reply *InstallSnapshotReply,
) bool {
	return rf.peers[server].Call(
		"Raft.InstallSnapshot",
		args,
		reply,
	)
}

// 实现每个 Follower 的长期 worker
func (rf *Raft) replicationWorker(peer int) {
	for {
		select {
		case <-rf.done:
			return
		case <-rf.replicateNotify[peer]:
		}
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.replicateBusy[peer] = true
		rf.mu.Unlock()

		rf.replicateUntilStable(peer)

		rf.mu.Lock()
		rf.replicateBusy[peer] = false
		rf.mu.Unlock()
	}
}

// 通知Follower复制 worker
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.state != Leader {
		return
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		select {
		case rf.replicateNotify[peer] <- struct{}{}:
		default:
			// 已经有一个待处理通知，无需重复排队。
		}
	}
}

// 当某个复制 worker 因长延迟 reply 阻塞时，不能让它同时阻塞心跳。
// 这里发送独立的空 AppendEntries；回复只用于发现更高任期，
// nextIndex/matchIndex 仍由串行复制 worker 维护。
func (rf *Raft) sendIndependentHeartbeat(
	peer int,
	args AppendEntriesArgs,
) {
	var reply AppendEntriesReply
	if !rf.sendAppendEntries(peer, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		rf.resetElectionDeadlineLocked()
	}
}

// 空闲 worker 继续走正常复制流程；正在等待旧 reply 的 worker
// 则由独立心跳维持 Follower 的选举计时器，避免长回复乱序造成任期风暴。
func (rf *Raft) heartbeatOrNotifyReplication() {
	type heartbeatTask struct {
		peer int
		args AppendEntriesArgs
	}

	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	tasks := make([]heartbeatTask, 0, len(rf.peers)-1)
	rescuePeers := make([]int, 0, len(rf.peers)-1)
	now := time.Now()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		if !rf.replicateBusy[peer] {
			select {
			case rf.replicateNotify[peer] <- struct{}{}:
			default:
			}
			continue
		}

		prevLogIndex := rf.lastLogIndexLocked()
		prevLogTerm := rf.lastLogTermLocked()
		tasks = append(tasks, heartbeatTask{
			peer: peer,
			args: AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			},
		})

		// 断连期间发出的旧 Call 最长可能阻塞数秒。空心跳只能维持
		// 选举计时器，不能补日志或安装快照，因此定期额外启动一次
		// 带完整追赶逻辑的异步复制；旧回复由 sentNext/term 防护过滤。
		if now.Sub(rf.lastRescueSent[peer]) >= 500*time.Millisecond {
			rf.lastRescueSent[peer] = now
			rescuePeers = append(rescuePeers, peer)
		}
	}
	rf.mu.Unlock()

	for _, task := range tasks {
		rf.launch(func() { rf.sendIndependentHeartbeat(task.peer, task.args) })
	}
	for _, peer := range rescuePeers {
		rf.launch(func() { rf.replicateUntilStable(peer) })
	}
}

// 使用 Raft 的上层服务（例如一个 key/value 服务器）希望开始对下一条要追加到 Raft 日志中的命令达成一致。
// 如果当前这个服务器不是 Leader，就返回 false。
// 如果当前服务器是 Leader，那么就开始对这条命令进行一致性处理，然后立即返回。
// 这里并不能保证这条命令最终一定会被提交到 Raft 日志中，因为 Leader 之后可能会宕机，或者在新的选举中失去 Leader 身份。
// 第一个返回值：如果这条命令最终被提交，它会出现在日志中的哪个 index。
// 第二个返回值：当前服务器的 currentTerm。
// 第三个返回值：如果当前服务器认为自己是 Leader，则返回 true；否则返回 false。

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return -1, term, false
	}
	index := rf.lastLogIndexLocked() + 1
	rf.appendLogs(LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	// Leader 自己已拥有该日志
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	// 多节点中新日志此时只有 Leader 自己拥有，不可能形成多数派；
	// 单节点集群则必须在这里立即提交。
	if len(rf.peers) == 1 {
		rf.advanceCommitIndexLocked()
	}
	rf.mu.Unlock()
	// 不等待下一个 heartbeat，立即复制
	rf.broadcastAppendEntries()
	return index, term, true
}

func randomElectionTimeout() time.Duration {
	ms := 400 + rand.Int63()%300
	return time.Duration(ms) * time.Millisecond
}

// ticker()的作用：后台不断检查“是否应该发起选举”
func (rf *Raft) ticker() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-rf.done:
			return
		case <-ticker.C:
		}
		rf.mu.Lock()
		shouldStart := !rf.killed() && rf.state != Leader &&
			!time.Now().Before(rf.electionDeadline)
		rf.mu.Unlock()
		if shouldStart {
			rf.startElection()
		}
	}
}

func (rf *Raft) heartbeatTicker() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-rf.done:
			return
		case <-ticker.C:
			rf.heartbeatOrNotifyReplication()
		}
	}
}

// 上层服务或测试程序通过 Make() 创建一个 Raft 服务器。
// peers[] 保存包括当前服务器在内的所有 Raft 服务器端点，当前服务器的端点是 peers[me]；
// 所有服务器的 peers[] 数组顺序完全相同。
// persister 用于保存当前服务器的持久化状态，并且在初始化时包含最近一次保存的状态（如果存在）。
// applyCh 是测试程序或上层服务用来接收 Raft 所发送 ApplyMsg 消息的通道。
// Make() 必须快速返回，因此所有耗时较长的工作都应在 goroutine 中运行。

// Make作用：创建并初始化一个 Raft 节点
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *tester.Persister,
	applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.done = make(chan struct{})
	rf.stopped = make(chan struct{})
	// 初始化最基础状态
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	// snapshot 边界初始值
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	// 初始日志只有一个 dummy
	rf.logs = []LogEntry{{Term: 0, Command: nil}}
	rf.rebuildEncodedLogs()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// 恢复持久化 Raft state
	rf.readPersist(persister.ReadRaftState())
	//恢复 snapshot
	rf.snapshot = persister.ReadSnapshot()
	// 恢复 volatile apply 边界
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	// 初始化选举计时器
	rf.electionDeadline =
		time.Now().Add(randomElectionTimeout())
	//初始化 replication workers
	rf.replicateNotify =
		make([]chan struct{}, len(rf.peers))
	rf.replicateBusy = make([]bool, len(rf.peers))
	rf.lastRescueSent = make([]time.Time, len(rf.peers))

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		rf.replicateNotify[peer] = make(chan struct{}, 1)
	}
	// 所有恢复完成后再启动 goroutine
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.launch(func() { rf.replicationWorker(peer) })
	}
	// 后台检查是否需要开始选举
	rf.launch(rf.ticker)
	rf.launch(rf.heartbeatTicker)
	rf.launch(rf.applier)
	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	//ticker()触发到发起选举期间有个时间窗口，二次检查是为了防止在这个窗口中接到Leader的心跳
	if rf.killed() || rf.state == Leader ||
		time.Now().Before(rf.electionDeadline) {
		rf.mu.Unlock()
		return
	}
	// 1. 变成 Candidate
	rf.state = Candidate
	// 2. 开启新的 term
	rf.currentTerm++
	// 3. 给自己投票
	rf.votedFor = rf.me
	// currentTerm 和 votedFor 都属于持久化状态
	rf.persist()
	// 4. 保存当前 term，后面处理 RPC 回复时要用
	term := rf.currentTerm
	lastLogIndex := rf.lastLogIndexLocked()
	lastLogTerm := rf.lastLogTermLocked()
	// 5. 自己已经有一票
	votes := 1
	// 6. 重置选举超时
	rf.resetElectionDeadlineLocked()
	majority := len(rf.peers)/2 + 1
	// 单节点集群给自己投票后已经获得多数票
	becameLeader := votes >= majority
	if becameLeader {
		rf.becomeLeaderLocked()
	}
	rf.mu.Unlock()
	if becameLeader {
		rf.broadcastAppendEntries()
		return
	}
	// 7. 给其他服务器发送 RequestVote
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		server := i
		rf.launch(func() {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			// 对方的 term 更大，立即退回 Follower
			// 更高任期必须先处理，不能因为这是旧 RPC 的响应而忽略
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.resetElectionDeadlineLocked()
				rf.mu.Unlock()
				return
			}
			// RPC 返回时，自己可能已经不是这个 term 的 Candidate 了
			if rf.state != Candidate || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			if !reply.VoteGranted {
				rf.mu.Unlock()
				return
			}
			votes++
			won := false
			if votes >= majority {
				rf.becomeLeaderLocked()
				won = true
			}
			rf.mu.Unlock()
			if won {
				rf.broadcastAppendEntries()
			}
		})
	}
}
