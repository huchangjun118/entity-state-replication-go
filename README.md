# entity-state-replication-go

多人在线场景下的实体状态同步库。服务端维护共享世界的权威状态，客户端通过
**增量快照 + 视野过滤 + 本地预测** 在丢包、乱序、重复且带宽有限的网络上重建
一致的可见状态。

## 架构

```
types.go      基础类型：Vec2 / EntityState / Input
world.go      权威世界：输入校验、定序应用、确定性步进
snapshot.go   快照与增量（Diff / Apply）
aoi.go        视野（AOI）过滤
transport.go  模拟链路：丢包 / 重复 / 可变延迟（乱序）
server.go     服务端：会话、增量基准追踪、带宽退让
client.go     客户端：重建、插值、预测与平滑修正、基线重试
```

## 同步语义

### 权威状态与输入（world.go）

- 服务端是唯一权威状态源。客户端输入（`Input`）**不直接**改变共享状态，
  由 `World.SubmitInput` 排队、`World.Step` 统一应用。
- 输入按 `(ClientID, Seq)` 排序后应用，保证确定性顺序。
- 每个客户端的输入序号单调递增；重复或过期（`Seq <= lastApplied`）的输入
  被拒绝，不产生任何效果。
- 移动向量被钳制到单位长度，位置被钳制到世界边界，非法输入无法越界。

### 增量同步（snapshot.go）

- `Diff(base, cur)` 只把变化的实体（含进入/离开视野）放入下行流量。
- `Apply` 要求 `BaseTick` 与本地快照 tick 精确匹配，否则拒绝——重复投递
  因此天然是幂等的。
- 增量重建的结果与全量严格一致（`TestDeltaChurnConvergesToFull` 以高频
  增删随机压力验证）。

### 不可靠通道（server.go / client.go）

- 服务端为每个客户端记录"客户端已确认（ack）的可见状态"，增量始终以
  **最近一次被 ack 的状态** 为基准，因此单个增量包丢失不会破坏后续流。
- 客户端丢弃 `Tick <= 当前` 的过期/重复快照；base 不匹配的增量被丢弃，
  等待服务端基于 last-ack 重发，短暂丢包不会造成永久错位。
- 当未确认包数超过 `MaxUnacked`，服务端回退发送**全量快照**自愈。

### 视野过滤（aoi.go）

- 每个客户端只同步其控制实体周围 `ViewRadius` 内的实体。
- 实体进出视野通过增量自然表达（进入 → Updated，离开 → Removed），
  单个实体的进出不影响其他实体的同步。

### 带宽退让（server.go）

- `BytesPerTick` 限制每客户端每 tick 的下行字节数。超预算时按优先级裁剪：
  控制实体最优先，其次 `PriorityRadius` 内的近处实体，远处实体最后。
- 被裁剪的实体只是在客户端保持短暂陈旧，下一 tick 会补上；服务端记录的
  是"客户端实际将持有的状态"，因此裁剪不会造成可见状态回退。
- 旧快照可被丢弃（未确认即被更新的增量取代），但客户端可见 tick 单调不减。

### 预测与修正（client.go）

- `SendInput` 立即作用于本地预测，不等服务端确认。
- 每个增量携带服务端已应用的输入序号 `InputSeq`；客户端以此回滚到权威
  状态并重放未确认的输入（reconciliation）。
- 预测与权威的偏差通过 `renderErr` 偏移量按 `CorrectionRate` 逐帧吸收，
  修正不产生可见跳变，也不干扰其他实体的插值。
- 远程实体按 `InterpDelayMs` 延迟在最近两个快照间插值渲染。

### 中途加入与基线

- 握手使用随机 nonce，重复/重试的 Hello 幂等，不会创建重复会话或实体。
- 客户端在收到完整基线（Full 快照）之前 `Ready() == false`，
  `RenderState` 返回 nil，不会呈现半初始化状态。
- 基线或欢迎包丢失时，客户端按 `BaselineRetryMs` 周期重试；
  失败期间不残留任何脏状态。

## 可配置项

### ServerConfig

| 字段 | 含义 | 默认建议 |
|---|---|---|
| `World` | 世界参数（移速/血量/边界） | — |
| `ViewRadius` | 视野半径 | 按玩法 |
| `MaxUnacked` | 未确认增量超过此值回退全量 | 5 |
| `BytesPerTick` | 每客户端每 tick 下行预算（0 = 不限） | 按带宽 |
| `PriorityRadius` | 关键实体优先级半径 | 按玩法 |

### ClientConfig

| 字段 | 含义 | 默认建议 |
|---|---|---|
| `InterpDelayMs` | 远程实体插值延迟 | 100 |
| `CorrectionRate` | 每帧吸收的预测误差比例 [0,1] | 0.2 |
| `BaselineRetryMs` | 基线/握手重试间隔 | 1000 |

### LinkConfig（传输模拟）

| 字段 | 含义 |
|---|---|
| `Loss` | 丢包概率 [0,1] |
| `Duplicate` | 重复概率 [0,1] |
| `MinDelay`/`MaxDelay` | 投递延迟区间（ms），延迟抖动产生乱序 |
| `Seed` | 随机种子（测试可复现） |

## 运行测试

```sh
go test ./...
```

覆盖：输入去重/定序、增量与全量一致性、实体高频增删、丢包/乱序/重复
下的收敛、视野进出、带宽退让与优先级、预测立即生效与平滑修正、中途
加入与基线重试。
