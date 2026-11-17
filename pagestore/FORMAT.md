# PageStore — 持久格式与恢复协议说明

仅依赖 Python 标准库的页式事务存储。本文档定义磁盘布局、WAL 协议、
检查点内容与恢复算法，并明确**支持的故障模型边界**。

## 1. 支持的故障模型（重要）

支持的是：

* **进程崩溃 / 硬退出**（`os._exit`、掉电导致的进程死亡），即内存丢失但
  已通过 `fsync` 持久化的字节保留；
* 在该模型下，假定文件系统正确履行 `fsync`，且**一次已完成的单页发布
  （临时文件 fsync → 原子 rename → 目录 fsync）不会撕裂**。

**不宣称**解决：磁盘永久损坏、静默位腐烂且无校验外的修复能力、固件把
`fsync` 撒谎成空操作、页发布的原子替换在硬件层被撕裂。遇到校验失败、
非法持久引用或损坏页，引擎**明确报错并定位，绝不清空重建或静默跳过**。

## 2. 目录布局

```
<store>/
  meta                 带帧的 JSON：页/槽数量、初值、预算、段大小
  writer.lock          flock 独占写锁（第二写进程被拒绝）
  pages/
    p-00000000         每页一个带帧文件
  wal/
    seg-00000001       滚动 WAL 段，只含带帧记录
    seg-00000002 ...
  cp/
    cp-0000000001      历次完整检查点（编号、不可变）
    current            指向最新完整检查点的原子软引用（同内容副本）
    cp-*.new           检查点发布中途的暂存（崩溃后不被选用）
  faults/              外部故障注入标记
```

## 3. 统一帧格式（页、WAL、meta、检查点共用）

```
MAGIC(4)="PSMG" | LEN(4,大端) | CRC32(4) | PAYLOAD(LEN)
```

CRC 覆盖 `MAGIC+LEN+PAYLOAD`。读取时：

* 尾部凑不齐一整帧 → `IncompleteTail`，保留此前有效前缀，重开时截断；
* 帧完整但 CRC 不符 → `CorruptionError`，附带文件与偏移，**硬失败**。

## 4. 页格式（一次页写回到底包含什么）

页 payload：`"PSPG" | version u16 | page_id u32 | slot_count u32 | 槽...`
每个槽 **42 字节**，保存**三个值 + 归属 + 提交者**：

| 字段 | 含义 |
|---|---|
| `state` u8 | 0=clean，1=pending（存在未提交值）|
| `pending_owner` i64 | 拥有 pending 值的事务 id，0 表示无 |
| `last_committer` i64 | 产生 `committed` 的事务 id（重放幂等判据）|
| `committed` i64 | 所有读者可见的最近已提交值 |
| `pending` i64 | 拥有者事务的工作值 |
| `prev` i64 | 取锁那一刻 `committed` 的值（**按槽撤销资料**）|

**一次页写回包含整页全部槽的这三类值，而不是只写提交后的旧值。**
因此带未提交修改的页可以被 `flush_page` 或缓存淘汰真实落盘：磁盘上
确实能读到该事务的 pending 值，同时其它槽的 committed 值独立保留。

### 已提交读取怎样避开活动写者

任何无事务（或别的事务）的 `read` 只读槽内的 `committed` 字段，绝不读
`pending`；只有 `pending_owner == 自己` 的事务才读 `pending`。于是同一页
上：

* T1 占槽 X（pending）、T2 占槽 Y（pending）可交错，互不读见；
* T2 先提交只把 Y 的 `pending→committed`、清 Y 的 owner，**不触碰 X**；
* T1 失败时只把 X 恢复成 X 自己的 `prev`，不会用整页旧镜像覆盖掉 T2。

冲突粒度是**槽**而非页：同槽被其它活动事务占用时显式抛
`ConflictError`（不实现等待）；同页不同槽始终可并发。

## 5. WAL 记录协议

记录体首字节为类型，整数大端、有符号 64 位：

| 类型 | 载荷 | 语义 |
|---|---|---|
| `TXN_BEGIN=1` | txid | 事务开始 |
| `SLOT_WRITE=2` | txid,page,slot,**全量值** | 槽的完整后像，非增量 |
| `COMMIT=3` | txid | 持久提交决定 |
| `ABORT=4` | txid | 用户中止决定 |
| `REC_ABORT=5` | txid | 恢复写出的中止决定 |

关键设计：

* `SLOT_WRITE` 存**全量值**。同事务多次 `set`/`add` 只留下每槽最后一个
  全量值，重放按 `(txn, slot)` 末值安装，**加法不可能被重放两遍**。
* `COMMIT/ABORT` 先 `fsync` 才算决定持久。提交确认成功前，决定及恢复
  所需资料已落盘，但**不要求数据页同时落盘**。
* 决定已 fsync、答复前崩溃 → 重开按已提交处理，同 txid 查到同一决定。
* 恢复自己写的是 `REC_ABORT`；再次恢复看到它就是“已发布决定”，不会被
  当成一笔新业务操作。

## 6. 检查点（活动事务可跨越）

检查点是带帧 JSON，**不要求**活动事务先结束、也不要求刷完脏页：

```json
{
  "version": 1, "seq": N,
  "wal_position": {"segment": s, "offset": o},
  "next_txn_id": k,
  "active":  {"<txid>": [[page,slot], ...]},      // 活动事务持有的槽锁
  "decisions": {"<txid>": "committed|aborted"},   // 已知持久决定
  "dirty_pages": [page, ...],                     // 持久副本可能陈旧的页
  "dirty_slot_images": {"<page>": {"<slot>": 值}} // 脏页的已提交槽映像
}
```

`dirty_slot_images` 让检查点**自包含**：恢复只扫检查点之后的 WAL，对一个
从未落盘的页也能仅凭检查点内的已提交映像物化，无需回溯更早日志。

发布三阶段，任一阶段崩溃都留有完整有效起点：

1. 写 `cp-N.new` 并 fsync；
2. rename 成 `cp-N`、fsync 目录（此刻才成为候选，故障点
   `checkpoint_during_switch` 位于此之后）；
3. 原子替换 `current`、fsync 目录。

阶段 1/2 崩溃 → 只有 `.new`/未挂名文件，不被选用；阶段 3 崩溃 → 旧
`current` 完整。重开优先读 `current`，损坏则回退到编号最大的完整
`cp-N`，都无效才硬报错。

## 7. 恢复算法（从检查点起步，不做全量历史重放）

1. **选起点**：`select_latest` 取一份完整检查点，记录其 WAL 位置。
2. **分析**：从该位置 `seek` 后只读 WAL 后缀（统计里能看到磁盘读取范围，
   历史前缀字节根本不读）。重建活动表、锁、决定、每槽最后写入与脏页集。
3. **决定**：仍活动且无持久决定的事务，写 `REC_ABORT` 并 fsync（中止
   持久化）。
4. **页处置**：受影响页 = 检查点脏页 ∪ 后缀日志涉及页 ∪ 检查点活动锁
   涉及页 ∪ **磁盘上任何仍带 pending 状态的持久页**。逐页逐槽：
   * pending 且 owner 已提交 → `pending→committed`（按 `last_committer`
     幂等）；
   * pending 且 owner 中止/无决定 → 该槽恢复 `prev`；
   * 后缀日志里的已提交全量写入，仅当 `last_committer < txid` 才安装；
   * 缺失页用初值 + 检查点槽映像物化。
5. 发布一份位于已解析 WAL 终点的新检查点，然后才开放业务读取
   （恢复完成前 `read` 抛 `StorageClosedError`）。

### 恢复中途再次崩溃的幂等性

* 已写的 `REC_ABORT` 是持久决定，二次恢复直接识别，不重复当新操作；
* 重做受 `last_committer`/owner 守卫，已处置的槽第二次为空操作；
* 页发布是原子的，半份页文件不会出现；
* 三个稳定故障点 `recovery_after_record_scan / recovery_mid_cleanup /
  recovery_mid_page_write` 可被外部反复 arm，收敛结果恒定。

## 8. 缓存预算与淘汰

`cache_budget` 是驻留页数硬上限。载入新页前若已达预算，按 LRU 选受害者
淘汰；**淘汰即发布**，且即使该页含未提交 pending 也照常写盘（页格式
天然支持）。测试通过断言 `len(cache) <= budget` 验证真实遵守，而非仅
把脏页一直留在内存。

## 9. 持久顺序与崩溃取证

* 页发布：临时页文件 `write+fsync` → `os.replace` → 目录 `fsync`。
* WAL：追加后由 commit/abort/checkpoint 显式 `fsync`；段滚动同步目录。
* 正常 `close` 会刷页并干净关库，**崩溃测试一律用 `os._exit`/故障标记
  硬杀，绝不以 close 冒充崩溃**。
* `bin/ps.py inspect-raw` 绕过恢复直接解析原始页文件，用于证明崩溃瞬间
  磁盘上确实存在未提交 pending、或某页确实尚未发布。

## 10. 在线备份（不冻结源库的一致副本）

`Storage.backup(target)` 把一个正在被并发写的存储目录导出为**可独立
`open_store`、继续读写、再次关闭重开**的副本。它从不把源库写入冻结到
整目录复制结束；复制期间提交、页刷写、淘汰、检查点全部继续交错。

### 10.1 单一逻辑时点（截止点）如何确定

1. **注册与版本保留**：备份开始即在源库登记 `bid`。此后每次页发布在
   `os.replace` 覆盖旧页文件前，把旧的不可变版本复制到该备份私有的
   保留目录 `<src>/.backup-staging/retention/<bid>/<pid>/v-<ver>`（普通
   文件复制，绝不是硬/软链接）。每页维护单调递增的版本号。
2. **屏障（唯一短临界区）**：在存储事务锁内 `fsync` WAL，并按当时 WAL
   位置 `(segment, offset)` 发布一份普通检查点。由于所有 WAL 追加与
   提交 `fsync` 都在同一把锁内，`(segment, offset)` 必是精确帧边界，
   且其之前的每条 COMMIT/ABORT 都已完整落盘。备份在此钉住：检查点序号、
   截止位置、每页屏障版本号、按**持久确认顺序**排列的决定表、下一个
   txid。
3. **无锁复制**：之后所有字节复制都不持锁。某页当前版本仍等于屏障版本
   就直接读在线文件（复制后再复核版本，期间若被替换则丢弃结果改读保留
   版本）；版本已变则从保留目录取屏障版本。屏障之后才诞生的页在副本中
   缺失，由副本自己的恢复依据检查点槽映像物化。
4. **WAL 只复制截止前缀**：复制到截止段为止，且截止段只取前 `offset`
   字节。截止点之后的记录物理上不进入副本，恢复不可能看到它们。
5. **先验证后发布**：所有页帧 CRC、检查点与截止位置一致性、WAL 前缀逐
   帧解析且恰止于截止点、逐文件 sha256/大小全部在**未发布的暂存区**内
   校验通过后，才写 `backup.marker` 并把整目录原子 `rename` 到目标路径。
   此前任何失败/强杀都不会在目标路径留下看似完整的备份。

因此副本不是“各页各自复制时刻的拼装”：页（在线或保留的屏障版本）、
检查点、WAL 前缀、决定表全部对应屏障上同一个 `(segment, offset)`。

### 10.2 副本目录与 `backup.marker`

副本是标准 PageStore 目录（`meta / pages / wal / cp`），额外在根目录带
一份带帧 JSON `backup.marker`：

```json
{
  "version": 1,
  "backup_id": "bk-<pid>-<seq>",
  "cutoff": {"segment": s, "offset": o},
  "checkpoint_seq": N,
  "next_txn_id": k,
  "committed_txids": [..按持久确认顺序..],
  "aborted_txids":   [..],
  "page_versions": {"<page>": <屏障版本>},
  "files": [{"path": "...", "size": n, "sha256": "...", "source": "live|retained"}]
}
```

独立驱动可只读 marker 与公开提交确认序列对账：`committed_txids` 是该
截止点内已持久提交事务的确认顺序；副本首次 `open_store` 先完成自身恢复
（只暴露截止点内已提交值，截止点仍活动的事务被副本恢复中止），随后即可
分配新事务 ID、提交、关闭重开。

### 10.3 并发边界

* 所有会改共享状态的公开操作（begin/read/set/add/commit/abort/
  flush_page/checkpoint/close/inspect）都在可重入的 `_txn_lock` 内；
  槽级冲突语义不变，同页不同槽仍可并发。
* 备份只在登记、屏障、注销三个短段落持锁，文件复制、CRC 校验、rename
  均不持锁，因此不会把写入暂停到复制结束。
* 保留目录在备份注销时随暂存区一并清除；进程在备份中途死亡后，重开时
  `sweep_staging` 删除整个 `.backup-staging`（内存中已无该备份，残树
  永不可能再被发布），源库照常恢复可用。

### 10.4 备份故障点与失败语义

稳定故障点（`faults/<POINT>.fault`，支持 `exit` 硬杀、`block` 挂起等待
释放、`event` 触达留痕三种模式）：
`backup_after_barrier / backup_mid_page_copy / backup_after_page_copy /
backup_after_wal_copy / backup_before_publish / backup_after_publish`。

* 目标路径已存在、暂存期写入失败（含注入的 ENOSPC）、任一来源页/WAL/
  检查点校验失败或引用缺失 → 抛 `BackupError`/`CorruptionError`/
  `IllegalReferenceError`，删除暂存区，**源库不受影响且可继续使用**，
  目标路径不存在有效备份，修正后可重新备份。
* `backup_after_publish` 之后被杀：有效备份已经发布，副本可独立打开；
  源库同样可重开。
* 备份产物不含任何指向源目录的链接，源目录删除后副本仍独立可恢复。
