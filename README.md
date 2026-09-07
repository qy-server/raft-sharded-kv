# 基于 Raft 的分布式分片 KV 存储系统

基于 **MIT 6.5840（原 6.824）2026 课程框架**完成的 Go 分布式系统学习项目。核心实现包括 Raft 共识、复制状态机、版本化 KV 服务、写请求去重、快照恢复及分片迁移，使用课程测试框架模拟丢包、网络分区、节点重启和控制器接管等故障。

本项目以协议正确性、故障恢复和并发控制为重点，运行入口以实验测试为主，不是可直接部署的生产数据库。

## 核心能力

| 模块 | 实现内容 |
| --- | --- |
| Raft | 随机超时选举、任期与投票管理、日志复制、冲突回退、多数派提交 |
| 状态恢复 | 经课程 Persister 接口保存和恢复 Raft 状态，支持日志快照及 InstallSnapshot |
| 复制状态机 | 将已提交日志按序应用到业务状态，通过操作身份匹配请求等待者 |
| 复制 KV | Get/Put 均经 Raft 排序；通过键版本号实现条件写入 |
| 请求去重 | 使用 ClientID、Sequence 和结果缓存实现 at-most-once 写入语义，支持同一客户端的并发乱序请求 |
| 分片 KV | 配置查询、请求路由、冻结与安装分片、发布配置及清理旧副本 |
| 控制器恢复 | 使用 Owner/Epoch、版本 CAS 和待完成迁移记录处理接管，防止过期控制器修改元数据 |
| 生命周期 | 提交等待超时、Leader 轮询、停止信号传播、后台 goroutine 退出与回收 |

仓库还包含 MapReduce、基础单节点 KV 和分布式锁实验。本地版本的实验编号为 Lab 1 MapReduce、Lab 2 KV/Lock、Lab 3 Raft、Lab 4 RSM/复制 KV、Lab 5 分片 KV，不应直接套用旧版 6.824 的实验编号。

## 关键设计

### 读写语义

`Get(key)` 返回值和版本，`Put(key, value, version)` 仅在版本匹配时修改状态。新键以版本 0 创建，首次写入后版本为 1；后续成功写入递增版本。不存在的键、版本冲突分别通过 `ErrNoKey`、`ErrVersion` 表达。

复制 KV 和分片组不会绕过 Raft 直接返回本地读取结果。Get/Put 经过同一日志和状态机执行路径，以实现线性一致的操作顺序；多数派不可用时，不以返回未经确认的本地状态换取可用性。

### 跨重试的写请求去重

- 每个 Clerk 生成客户端身份，每个逻辑 Put 分配递增请求序列号；响应丢失、Leader 切换和分片重新路由时保留同一身份。
- 状态机按请求身份缓存确定的执行结果，包括成功与版本冲突。重复请求返回原始结果，不重复修改状态。
- 同一 Clerk 可同时存在多个未完成请求，因此不是只记录一个“最大已执行序列号”，而是分别记录尚未确认的请求结果。
- 客户端通过 `Ack` 确认连续完成的请求前缀。服务端清理对应回复，但保留确认水位，防止迟到请求在回复清理后再次执行。
- 去重状态与键值、版本一起进入快照及分片迁移数据，避免重启或迁移后丢失请求执行记录。

该去重机制针对复制 KV 和分片 KV 的写请求。基础单节点 `kvsrv1` 保留课程原有的条件写与 `ErrMaybe` 语义；Get 不承诺只执行一次。

### 超时与停止

KV 和分片组的 RPC handler 使用 `SubmitWithTimeout`，单次服务端提交等待为 1 秒。旧 Leader 即使仍能响应客户端连接，也不会让该次提交无限等待；客户端可继续尝试其他副本。超时不取消已进入 Raft 的日志，也不表示写入一定失败，因此重试必须保留请求身份。

底层 `Submit` 保持课程要求的不设截止时间行为。两者分开，避免破坏课程 RSM 在网络分区期间的阻塞语义。

Raft 使用停止信号、条件变量唤醒及 worker 跟踪管理生命周期。`Kill` 后拒绝新提交；`Done()` 表示停止已发起，`Stopped()` 表示被追踪的 Raft worker 已退出。调用方拥有 `applyCh`，Raft 不负责关闭它。

### 分片迁移

控制器先记录待完成配置变更，再依次冻结源分片、安装目标分片、发布配置和清理源分片。配置号及分片状态用于拒绝迟到、重复或过期的迁移操作；目标组同时接收数据、版本和去重状态。

控制器接管通过 Owner/Epoch 与版本 CAS 排除旧控制器，并继续完成遗留迁移。分片重分配复用课程提供的按分片数量均衡逻辑，不包含基于实时 QPS 或热点的调度。

## 目录结构

```text
.
|-- README.md
|-- Makefile                 # Course submission targets
|-- .check-build             # Course submission checker
`-- src/
    |-- Makefile             # Build and test targets
    |-- go.mod / go.sum
    |-- raft1/               # Raft implementation and tests
    |-- raftapi/             # Raft interface
    |-- kvraft1/             # Replicated KV
    |   |-- rsm/             # Replicated state machine
    |   `-- dedup/           # Write request identity and deduplication
    |-- shardkv1/            # Sharded client and integration tests
    |   |-- shardgrp/        # Shard group state machine and migration RPCs
    |   |-- shardctrler/     # Configuration and migration coordination
    |   `-- shardcfg/        # Configuration structures and balancing
    |-- kvsrv1/              # Single-server KV and lock lab
    |-- mr/                  # MapReduce coordinator and worker
    |-- mrapps/              # MapReduce test plugins
    |-- main/                # Course daemon entry points and test inputs
    |-- labrpc/ / labgob/    # Course RPC and serialization framework
    `-- tester1/ / kvtest1/ / models1/  # Testing and consistency models
```

课程提供了协议骨架、接口、RPC/序列化、测试框架、测试数据及部分辅助算法；本仓库在此基础上补全并扩展相关实现，也包含局部框架适配。上述课程组件不作为从零原创成果，具体实现以源码为准。

## 运行环境

- `go.mod` 声明 Go 1.22；已验证环境为 Go 1.26.0、Linux/amd64、WSL Ubuntu 22.04。
- 需要 GNU Make、Bash，以及支持 `-race`/CGO 的 C 编译器，例如 GCC。
- 课程测试使用 Unix domain socket；MapReduce 使用 Go plugin。Windows 用户请在 WSL 中构建与测试，不使用 Windows 原生终端直接运行整套实验。
- 首次下载依赖需要能够访问配置的Go module源。主要外部测试依赖为 Porcupine 线性一致性检查器。

以下命令均在 Linux/WSL shell 中执行。先从仓库根目录进入 Go 模块目录，后续命令保持在该目录运行：

```bash
cd src
go version
go mod download
```

根目录的 Makefile 用于课程提交；日常构建和测试使用 `src/Makefile`。

## 构建与测试

### 快速验证

构建 Raft 测试进程并运行一次初始选举测试：

```bash
make RUN="-run '^TestInitialElection3A$' -count=1" raft1
```

课程 Makefile 默认启用 `-race`。按模块执行测试：

```bash
make raft1
make rsm1
make kvraft1
make shardkv
```

其他实验可分别执行 `make kvsrv1`、`make lock1` 和 `make mr`。请使用指定入口，直接在 `src` 执行 `go test ./...` 会包含多个独立 main/plugin 文件，不是本仓库的全量测试方式。

### 复现 KV/Raft 完整验证

先构建测试框架需要启动的服务器进程：

```bash
make kvsrv1-build raft1-build rsm1-build kvraft1-build shardkv-build
```

运行主套件，将高负载内存测试单独执行，减少并行测试竞争资源造成的超时：

```bash
go test -race -count=1 -p=3 -timeout=20m \
  -skip '^TestMemPutManyClientsReliable$' \
  ./raft1 ./kvraft1 ./shardkv1 ./kvraft1/rsm ./kvraft1/dedup \
  ./shardkv1/shardctrler ./shardkv1/shardgrp ./shardkv1/shardcfg \
  ./kvsrv1 ./kvsrv1/lock ./labrpc ./labgob

go test -race -count=1 -p=1 -timeout=5m \
  -run '^TestMemPutManyClientsReliable$' ./kvsrv1
```

这里的 `-timeout` 是 Go 测试进程限制，没有修改课程测试自身的断言和计时限制。测试耗时受机器负载影响，建议避免同时运行多个完整套件。

### 重复回归测试

完成上述服务器构建后，重复执行新增的去重、超时、故障切换和停止回归测试：

```bash
go test -race -count=10 -p=2 -timeout=5m \
  -run '^Test(ClientRecovers|Dedup|ConcurrentSequences|OutOfOrderReplay|FailureReplay|Kill|Submit)' \
  ./raft1 ./kvraft1/... ./shardkv1/shardgrp
```

重点用例：

| 测试 | 验证场景 |
| --- | --- |
| `TestConcurrentSequencesAndAcknowledgements` | 并发请求序列号唯一，确认水位不越过未完成请求 |
| `TestOutOfOrderReplayAndRetirement` | 请求乱序执行、回复缓存清理与迟到重试 |
| `TestDedupSnapshotAndCachedResults` | 快照恢复后保留成功、失败回复及确认水位 |
| `TestDedupReplyLostAcrossLeaderFailure` | 丢弃成功响应并关闭原 Leader，再次重试不重复修改状态 |
| `TestDedupSurvivesMigrationAndRestore` | 分片迁移和恢复后仍能识别重复写入 |
| `TestClientRecoversFromReachableOldLeader` | 客户端仍可访问孤立旧 Leader 时能够切换到多数派 |
| `TestKillStopsBlockedApplyAndRejectsWork` | apply 阻塞时可停止，停止后拒绝新工作 |
| `TestSubmitTimeoutCleansWaiter` | 提交超时后清理等待者 |

## 验证记录

以下为 **2026-09-06** 在上述环境中的本地测试记录，不代表持续集成状态或对所有执行历史的正确性证明。

| 范围 | 结果 |
| --- | --- |
| KV/Raft 主套件 | 124/124 项测试通过，启用 `-race` |
| 单独高负载内存测试 | 1/1 通过，20,000 个 Put 客户端，用例耗时 88.40 秒 |
| 新增回归测试 | 12 项各重复 10 次，120/120 次执行通过 |
| 数据竞争检测 | 本轮日志未报告 `WARNING: DATA RACE` |

主套件和单独内存测试合计 **125 个不同测试**，其中包含 12 个新增回归测试。MapReduce 不计入这轮 125 项测试结果。

主套件中有 3 次线性一致性检查器超时：`TestManyConcurrentClerkReliable5A` 中 1 次，`TestSnapshotRecoverManyClients4C` 中 2 次。课程测试将对应历史按通过处理，但检查器没有完成确定性验证。因此，这些结果不能表述为“所有历史均已严格验证线性一致”。

## 已知边界

- **持久化介质**：使用课程 Persister 接口完成状态保存与恢复，未自行实现磁盘 WAL、fsync 或存储介质故障恢复。
- **可用性**：网络分区时需保留多数派才能推进共识；1 秒限制是服务端单次提交等待，不是客户端端到端截止时间，也未提供用户级取消接口。
- **去重生命周期**：客户端身份未在客户端进程重启后持久化；确认水位不按时间过期，长期创建大量一次性客户端仍可能增加会话状态。
- **迁移停顿**：迁移中的分片会冻结并触发重试，不是零停顿迁移，也未实现跨分片事务。
- **控制器元数据**：依赖单节点 `kvsrv1`，控制器对象可接管不等于元数据服务本身具备 Raft 多副本容错。
- **停止语义**：课程 `labrpc` 的在途 RPC 不能主动取消，`Stopped()` 需等待这些调用自然结束；不承诺瞬间终止全部 RPC。
- **部署能力**：未实现 Raft 成员动态变更、混合版本滚动升级保证、认证鉴权或生产级监控运维。

## 课程与参考资料

- [MIT 6.5840 课程主页](https://pdos.csail.mit.edu/6.5840/)
- [课程说明与协作政策](https://pdos.csail.mit.edu/6.5840/general.html)
- [Lab 3: Raft](https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html)

本仓库是课程学习与实现展示项目，不代表 MIT 官方实现。请保留课程来源及已有声明，不将课程框架、测试或辅助算法标为全部个人原创。
