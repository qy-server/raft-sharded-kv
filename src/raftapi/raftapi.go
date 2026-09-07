package raftapi

// Raft 接口
// 任何 Raft 实现只要提供下面这些方法
// 就可以被上层服务或测试框架当作一个 Raft 节点使用
type Raft interface {
	// Start 尝试让 Raft 集群对一个新的日志条目达成一致。
	// command 是上层服务希望写入 Raft 日志的命令。
	// 返回值分别为：
	// 1. 该日志条目预计所在的日志下标 index
	// 2. 当前 Leader 所处的任期 term
	// 3. 当前节点是否认为自己是 Leader
	Start(command interface{}) (int, int, bool)

	// GetState 获取当前 Raft 节点的状态。
	// 返回值分别为：
	// 1. 当前任期 currentTerm
	// 2. 当前节点是否认为自己是 Leader
	GetState() (int, bool)
	// Snapshot 用于快照功能，在 Lab 3D 中使用。
	// index 表示快照包含到哪个日志下标。
	Snapshot(index int, snapshot []byte)
	// PersistBytes 返回当前 Raft 持久化状态占用的字节数。
	// 测试框架可以通过它判断日志持久化数据是否过大，
	// 以及 Raft 是否正确执行了日志压缩和快照。
	PersistBytes() int
}

// 当一个 Raft 节点发现连续的日志条目已经被提交时， 它应该通过 Make() 函数传入的 applyCh， 向上层服务或测试程序发送 ApplyMsg。
// 当 ApplyMsg 中包含一个新提交的普通日志条目时，应该将 CommandValid 设置为 true。
// Snapshot 相关字段会在后面的实验中使用。
// CommandValid 和 SnapshotValid 中，应该有且只有一个为 true：
// - CommandValid == true：表示应用普通日志命令
// - SnapshotValid == true：表示安装或应用快照
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
