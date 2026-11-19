# cdcsqlite — 固定 Schema 快照 + CDC 的 SQLite 新基线重建

本包在不改动 goavro 既有 API 的前提下，提供一条「全量快照 + 有序变更流」到
SQLite 基线的落库管线。所有消息都用**同一个固定 Avro Schema**（`WireSchema`）
做真实二进制编解码，运行期不接受任何其它 Schema。

## 线协议（固定 Schema）

`WireSchema` 定义一个 `Envelope` 记录，字段语义：

| 字段 | 含义 |
| --- | --- |
| `gen` | 快照代次。重建代次必须严格大于已发布代次；`0` 表示与具体重建无关的普通增量。 |
| `kind` | `SNAPSHOT_ROW` / `SNAPSHOT_END` / `UPSERT` / `DELETE`。 |
| `seq` | 单调、连续（无缺口）的 CDC 日志序号。快照行取 `0`，真实 CDC 取 `>0`。 |
| `cut` | 快照水位线 L：快照对 CDC 位置 L 一致，`seq <= L` 的变更已包含在快照内。 |
| `row_count` | 生产者声称的快照行数；`SNAPSHOT_END` 给出权威值，`0` 表示刻意的空人口。 |
| `digest` | 可选快照摘要。 |
| `row` | 固定 Schema 的 `User` 行（`id` 定长主键，`email` 可空，`active`、`score`）。 |

`DecodeEvent` 严格要求一次调用恰好消费一个信封：尾随字节、截断或损坏载荷一律报错。

## 关键保证

1. **重建期间消费者始终读旧完整目标。** 快照行与 CDC 先写入独立的
   `staging.db`，`target.db` 在发布前一字不动。读路径走独立只读连接 + WAL，
   读连接永远只看到已提交快照，绝不会看到半张新表。
2. **快照（cut L）与连续有序 CDC 可交错。** `(L, H]` 的事件按
   `L+1, L+2, …, H` 无缺口缓冲；`seq <= L` 的事件已在快照内，安全跳过。
3. **双证据齐备才发布。** 只有同时满足 (a) 收到 `SNAPSHOT_END` 完整证据且行数
   与实际暂存一致，(b) `(L, H]` 前缀连续齐备，`TryPublish` 才会提交；否则返回
   `ErrNotPublishable`，旧目标继续服务。
4. **新表内容与断点同次生效。** 发布是 target 上的单个跨库事务：建新表 →
   灌入快照 → 顺序回放 CDC → 原子换表 → 同事务写 `published_gen`/`watermark`。
   接收（解码）进度绝不等于落库进度，每个事件都在对应事务 `COMMIT` 后才算被接受。
5. **代次、重复与冲突显式化。** 非当前代次的事件报 `ErrGenerationMismatch`；
   回退代次报 `ErrGenerationRollback`；完全相同的重发识别为幂等重复并跳过；
   同键不同载荷默认 `ConflictReject` 报 `ErrDuplicate`，可选 `ConflictLastWins`。
6. **重开继续。** 暂存状态全部持久化在 `staging.db`，进程重启后 `Open` 自动恢复
   未完成重建并继续；已发布代次的遗留暂存文件会被识别并丢弃，旧代数据绝不混入
   或回退目标。
7. **H 之后按序增量。** 发布后仅接受 `watermark+1`；已发布序号重发为重复，跳号
   报 `ErrOutOfOrder`，每条增量与新水位线同事务提交。
8. **空快照正确替换旧人口、失败保旧。** 空 `SNAPSHOT_END`（`row_count=0`）会把目标
   替换为空表再加增量；任何失败都回滚，旧完整目标保持不变。
9. **固定主键/Schema。** 目标表 `users(id INTEGER PRIMARY KEY, email TEXT,
   active INTEGER NOT NULL, score REAL NOT NULL)` 与元数据表 `cdc_meta` 固定不变。

## 典型用法

```go
b, err := cdcsqlite.Open(cdcsqlite.Config{Path: "/var/data/baseline"})
if err != nil { return err }
defer b.Close()

if err := b.BeginRebuild(gen); err != nil { return err } // gen > 已发布代次

for payload := range stream {                 // payload 是一条 Avro 二进制信封
    if _, err := b.Apply(payload); err != nil {
        // 解码失败/乱序/代次不符/冲突：本次调用未落任何库，旧目标不受影响
        return err
    }
}

if _, err := b.TryPublish(); err != nil {
    // 证据或前缀不全：消费者仍读旧目标，可继续补数据后重试
    return err
}

// 发布后继续增量：只接受 watermark+1
```

## 实现说明

沙箱内无可用 Go SQLite 驱动且离线，`sqlite.go` 通过 cgo 直接声明并链接系统
`libsqlite3`（`#cgo LDFLAGS: -l:libsqlite3.so.0`），仅暴露本包所需的最小符号集，
不依赖任何额外 Go module。
