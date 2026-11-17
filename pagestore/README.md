# PageStore — 从零实现的页式事务存储与重启恢复

纯 Python 标准库实现的、真正读写文件的事务存储引擎。**不**是读别人日志
生成报告，**不**给 SQLite 包一层，也不调用任何现成数据库/键值引擎承担
事务、日志或恢复。

## 能力

* 固定页数 × 固定槽位数，槽内为有符号 64 位整数，初值可配置。
* 事务 API：`begin / read / set / add / commit / abort`，外加显式
  `flush_page / checkpoint / close` 与重新 `open`。
* 事务 id 由存储分配，可按 id 查 `committed / aborted` 决定。
* 多事务在单进程内交错；第二写进程被 `flock` 拒绝。
* **槽级**冲突：同槽未结束写者显式拒绝（不等待）；同页不同槽两个事务
  可交错并各自提交/中止，绝无整页独占或整库长事务串行化。
* `add` 溢出在写入前拒绝，无半次写入。
* 可调驻留页预算，淘汰真实遵守预算，未提交页也能被淘汰落盘。
* 提交决定先持久、数据页可后刷；检查点允许活动事务跨越。
* 进程崩溃恢复：已确认提交全保留、无持久提交决定者全不生效，支持恢复
  中途再次强杀且幂等收敛。
* **在线备份**：存储可被并发写时导出一致、独立、可继续读写的副本；
  备份与提交/刷页/检查点交错，绝不冻结写入。每份副本带对应单一逻辑
  截止点的 `backup.marker`（含按持久确认顺序的提交序列与逐文件哈希），
  复制中途强杀、目标已存在、空间不足、来源校验失败均明确失败且不产生
  半成品，修正后可重复备份。
* **在线历史回收**：`reclaim_plan()` 只读预览可删的 WAL 段/旧检查点与
  每个保留文件的钉子（长事务撤销映像、在线备份截止点、恢复依据），
  `reclaim()` 重新判断依赖后真实 `unlink` 释放空间；先持久化替代恢复
  依据，再写清单，最后在业务锁外批量删除。历史决定与已确认值不变。
* 帧长度 + CRC 保护：尾记录截断保留有效前缀；完整记录损坏/页损坏/非法
  持久引用一律明确定位并拒绝，不清空重建。

## 目录

```
pagestore/          引擎包
  framing.py        统一长度+CRC 帧、原子文件发布
  records.py        WAL 记录协议
  wal.py            滚动段 WAL，从显式位置扫描
  pageio.py         页格式（committed/pending/prev 三值 + 归属）
  checkpoint.py     检查点三阶段原子发布与选择
  recovery.py       分析 + 按槽撤销/重做，幂等重入
  backup.py         在线备份：屏障截止点、版本保留、截止段WAL、校验发布
  reclaim.py        在线历史回收：预览规划、替代依据/清单/无锁删除
  crash.py          稳定故障注入点（含备份/回收 block/event 模式）
  store.py          缓存预算、事务、提交/中止、open/create、backup、reclaim
  cli.py            命令行演练
bin/ps.py           隔离模式入口（进程级演练）
tests/              进程驱动测试、独立参考模型、差分/损坏/再崩溃测试
  backup_reference.py  备份专用独立内存模型（枚举合法截止位置）
  backup_worker.py     确定性交错的备份场景独立进程
  reclaim_worker.py    多轮写入+回收+跨轮长事务的连续演练进程
  reclaim_expect.py    回收专用独立期望（公开操作流 + 持久确认）
  recovery_oracle.py   独立格式重放（不 import 产品恢复/回收决策）
  test_backup.py       备份一致性、崩溃矩阵、失败处理与并发压力
  test_reclaim.py      回收预览/执行、收敛、崩溃/强杀、备份衔接
  evidence/            有界验证证据（含逐轮真实文件名/大小/原因/总字节）
  evidence/            有界验证证据（每次运行重生成）
FORMAT.md           持久格式、恢复协议与在线备份协议的完整说明
```

## 快速开始（Python API）

```python
from pagestore import create, open_store

st = create("/tmp/db", num_pages=8, slots_per_page=16,
            initial_value=0, cache_budget=4)
t1 = st.begin()
st.add(t1, 0, 0, 10)          # 页0槽0
t2 = st.begin()
st.set(t2, 0, 1, 20)          # 同页不同槽，可并发
st.commit(t2)
st.abort(t1)
st.read(0, 0)                 # 0（t1 已中止）
st.read(0, 1)                 # 20
st.close()

st = open_store("/tmp/db")    # 崩溃后也用同一入口，先恢复再开放读取
```

## 在线备份（在线、可重复、独立可开）

```python
st = open_store("/tmp/db")                # 源库保持可写，其他线程照常提交
marker = st.backup("/tmp/db-backup-1")    # 不冻结写入，返回截止标记
# marker["cutoff"]          -> {"segment": s, "offset": o} 单一逻辑时点
# marker["committed_txids"] -> 该时点前已持久提交事务的确认顺序
# 副本是普通 PageStore：独立打开、先恢复到截止点、再继续读写
bk = open_store("/tmp/db-backup-1")
t = bk.begin(); bk.add(t, 0, 0, 1); bk.commit(t); bk.close()
bk = open_store("/tmp/db-backup-1")       # 关闭重开，新决定仍在
```

副本打开后只暴露截止点内的已提交值；截止点仍活动的事务由副本自身恢复
中止；截止点之后的 WAL 记录物理上不进入副本。复制期间源库的页被替换
时，引擎自动保留屏障版本供备份读取，绝不用不同时刻的页状态拼装。命令
行等价入口：`python3 -I bin/ps.py backup <src> <target>`，用
`backup-marker <target>` 读取截止标记。完整协议见 `FORMAT.md` 第 10 节。

## 在线历史回收（预览 → 执行）

```python
st = open_store("/tmp/db")
plan = st.reclaim_plan()        # 只读：不发布检查点、不写、不删
# plan["removable"]            -> 现在就能安全释放的段/检查点与字节数
# plan["kept"] / ["kept_detail"] -> 保留的文件与 pinned 原因
# plan["recoverable_entry"]    -> 当前恢复入口（检查点 seq + WAL 位置）
# plan["keep_wal_range"]       -> 仍需保留的日志范围
# plan["active_pins"]          -> 旧事务只钉住自己槽的撤销映像
# plan["backup_pins"]          -> 在线备份的截止段/屏障检查点
res = st.reclaim()              # 重新判断依赖，然后真实 unlink
# res["removed"]  -> 实际移除的 WAL 段、检查点、总字节
# res["failed"]   -> 逐文件失败（权限等）；此时 res["ok"] is False
```

命令行等价入口：

```bash
python3 -I bin/ps.py reclaim /tmp/db --preview     # 只报告（不加写锁、零写入）
python3 -I bin/ps.py reclaim /tmp/db               # 实际释放
python3 -I bin/ps.py reclaim /tmp/db --keep-checkpoints 1
```

语义要点（完整协议见 `FORMAT.md` 第 11 节）：

* 只删 `wal/seg-*` 与 `cp/cp-*`；数据页、未知文件、已发布备份不动；
  是真 `unlink`，不搬进垃圾目录，不重建库。
* 执行先发布一份新的自包含检查点作为替代恢复依据（短同步），再写
  `reclaim.json` 清单，最后在**释放业务锁之后**批量删除；删除中途可用
  `--pause-at N` 停住，其它线程照常提交/检查点。
* 长事务、已提交未刷出页、已落盘未提交槽、多个不同阶段的在线备份都会被
  正确保护；一笔老事务只钉住自己的撤销映像，不会永久钉住整段历史。
* 崩溃后重开能区分“合法回收前缀”与“仍需日志缺失/CRC 损坏”；检查点
  不会回退到依赖已删日志的旧版本并悄悄丢数据。
* 文件编号只增不减：回收只 `unlink`，从不改名/回填编号，所以缺号是否
  合法完全由检查点的 `reclaim` 记录与保留区间决定。
* 检查点格式 v2 的槽映像是**完整**槽状态（含 `prev` 撤销值），因此
  恢复和回收都不需要检查点位置之前的任何 WAL 记录（v1 目录首次回收时
  自动升级）。

## 命令行演练（真实进程 + 真实故障）

```bash
python3 -I bin/ps.py create /tmp/demo/db --pages 4 --slots 3 --budget 2
python3 -I bin/ps.py phase  /tmp/demo/db main_demo --hard-exit --exit-code 98
python3 -I bin/ps.py inspect-raw /tmp/demo/db 0      # 看崩溃瞬间原始页
python3 -I bin/ps.py arm-fault /tmp/demo/db recovery_mid_page_write
python3 -I bin/ps.py read-all /tmp/demo/db           # 恢复到一半被杀(99)
python3 -I bin/ps.py read-all /tmp/demo/db           # 再次恢复，结果恒定
python3 -I bin/ps.py decisions /tmp/demo/db 1 2 3
```

## 主演练（题目要求的场景）

`main_demo` 阶段：同页 X=0/Y=0、另页 Z=0；T1 给 X 加 10 并保持活动，
T2 置 Y=20 并提交，刷出含两者变化的页；T3 给 Z 加 30 并提交但不刷 Z；
跨活动 T1 做检查点，然后**不 close 硬退出**。恢复结果 X=0、Y=20、
Z=30，T1 不生效、T2/T3 已提交；`inspect-raw` 同时取证崩溃前持久页确含
T1 未提交状态、Z 页确实尚未更新。恢复停在部分处置后再杀，继续开库
结果相同，add 不会再加一次。

## 跑全部测试

```bash
python3 -I tests/run_all.py
```

包含：进程驱动崩溃/重启实验（33 项）、损坏/截断/锁（16 项）、对独立
参考模型的随机差分（多 seed、逐槽比较）、三个恢复故障点各重复杀两次的
再崩溃矩阵、在线备份套件（75 项），以及在线历史回收套件（150 项，见下）。
压力数据由参数在运行时生成，仓库只保留小夹具、生成器与有界验证结果，
不含巨型数据库或日志。

### 在线备份演练覆盖的场景

`tests/test_backup.py` 全部以独立 OS 进程驱动，证据写入有界文件
`tests/evidence/backup_evidence.json`（外部进程退出码、页/WAL 读取范围、
提交序列、逐槽结果）：

* **交错主场景**：至少两页上的事务在复制期间交错；一个事务跨过截止点
  （截止时仍活动，副本恢复后不生效），另一个在截止点后提交；含活动修改
  的页在复制窗口内被刷盘；复制页文件期间对源库触发检查点。
* **独立核对**：`tests/backup_reference.py` 只用工作进程记录的公开操作流
  与它自己观察到的持久提交确认，枚举备份窗口内允许的截止位置，逐槽预测
  并核对副本值与 marker，**不 import 产品的日志分析或恢复代码**。
* **副本独立可用**：副本在独立目录打开、写入新事务、关闭、再次打开，
  并核对其决定与数据没有串回源库。
* **强杀矩阵**：在备份六个稳定故障点分别硬杀进程，随后重开源库（暂存
  残树被清扫、源库照常恢复），再重试备份成功；另验证发布后才被杀时副本
  已有效。
* **失败处理**：目标已存在（原样保留）、注入 ENOSPC 中途写失败、来源页
  CRC 损坏、截止点引用的 WAL 段缺失，均明确失败且不留有效半成品，修正后
  可重新备份。
* **并发压力与版本保留**：多写线程与重复备份并发；热页在复制期间被反复
  替换，验证备份经保留版本重建，安静库则直接读在线页，两条路径都有计数
  与逐文件 `source` 标记取证。

### 在线历史回收演练覆盖的场景

`tests/test_reclaim.py` 的期望只来自公开操作流 + 驱动自己观察到的持久
提交确认（`tests/reclaim_expect.py`），以及对磁盘格式的独立重放
（`tests/recovery_oracle.py`，**不 import** 产品的恢复/回收决策代码）：

* **预览只读**：整目录 size/mtime/名字集合前后完全一致；同时报告可删
  集合、每个保留文件的钉子、恢复入口与保留日志范围。
* **连续收敛**：小 WAL 段 + 10 轮写入/提交/检查点/回收，保留一个跨轮
  活动事务，交替制造“未刷出的已提交值”和“落盘的未提交值”。逐轮记录
  真实删除的文件名、字节数、floor、预览与执行的集合一致性；结束后
  WAL 收敛到 1 个段，历史决定仍在，`decision_metadata_bytes` 单列。
  长事务结束后再回收、强杀、重开，逐槽与决定对照独立期望（另有一个进程
  在“结束长事务→回收→不 close 硬退出”后重开核对）。
* **崩溃/强杀**：在回收的四个稳定点用真实进程硬杀（保留退出码 99），
  重开后识别为合法回收、继续回收并继续写入；外部进程在回收的四个点
  （替代依据持久化前/后、部分 unlink 后、全部 unlink 后）分别 `SIGKILL`
  （退出码 -9，真实记录），再验证重开、再次回收与继续写入。
* **锁与暂停**：在删除批次暂停时，另一线程完成 begin+commit+checkpoint
  并持久化。
* **备份衔接**：备份 A 停在屏障、期间穿插写入与回收，验证其截止段与
  屏障检查点受保护而不相关历史被删；备份 B 人为 ENOSPC 失败，确认保护
  解除且空间继续释放；A 完成后可独立打开且不含截止点之后的段。
* **失败与分类**：删除权限失败逐文件报告且 `ok=False`，修权限后重试真正
  释放；区分合法删除前缀、手工删掉仍需日志、损坏检查点引用、保留段 CRC
  失败四种情形；v1（无回收记录）目录可直接打开并首次回收。
* **回收后的备份**：只复制截止段，不把已删的整段历史补回副本；副本独立
  打开、写入、关闭重开。
* **作用域**：回收只删 `wal/seg-*` 与 `cp/cp-*`；数据页字节、store 根下
  未知文件、业务子目录、wal/ 里的非段文件、已发布备份目录全部原样保留。
* **并发压力**：3 个写线程持续提交/刷页，另一线程连续回收 3 秒，零异常，
  段确实被释放，重开后各槽仍是整数。
* **随机差分**：6 个种子 × 80 步随机公开操作（含冲突/溢出/中止），穿插
  检查点与回收，结束时逐槽、逐决定对照独立期望，并用独立重放核对。
* **跨回收长事务**：长事务先写、其后前缀段被真实删除，再分别提交/中止；
  值、决定与独立重放都正确（只钉撤销映像，不钉整段历史）。

## 独立参考模型

`tests/reference.py` 只消费公开事务操作流和“已持久决定”，自己重算每槽
期望值，**不 import 引擎的 WAL/页/恢复代码**。差分与崩溃测试逐槽比对
两个独立实现；持久边界由外部故障标记与明确的 fsync 完成信号核实。

## 故障模型边界

支持进程崩溃及本文档（见 `FORMAT.md` 第 1 节）描述的持久化模型：
fsync 被正确履行、单次页原子发布不撕裂。**不**宣称解决磁盘永久损坏。
