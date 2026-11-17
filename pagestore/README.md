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
  backup.py         在线备份：屏障截止点、版本保留、前缀WAL、校验发布
  crash.py          稳定故障注入点（含备份 block/event 模式）
  store.py          缓存预算、事务、提交/中止、open/create、backup
  cli.py            命令行演练
bin/ps.py           隔离模式入口（进程级演练）
tests/              进程驱动测试、独立参考模型、差分/损坏/再崩溃测试
  backup_reference.py  备份专用独立内存模型（枚举合法截止位置）
  backup_worker.py     确定性交错的备份场景独立进程
  test_backup.py       备份一致性、崩溃矩阵、失败处理与并发压力
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
再崩溃矩阵，以及在线备份套件（75 项，见下）。压力数据由参数在运行时
生成，仓库只保留小夹具、生成器与有界验证结果，不含巨型数据库或日志。

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

## 独立参考模型

`tests/reference.py` 只消费公开事务操作流和“已持久决定”，自己重算每槽
期望值，**不 import 引擎的 WAL/页/恢复代码**。差分与崩溃测试逐槽比对
两个独立实现；持久边界由外部故障标记与明确的 fsync 完成信号核实。

## 故障模型边界

支持进程崩溃及本文档（见 `FORMAT.md` 第 1 节）描述的持久化模型：
fsync 被正确履行、单次页原子发布不撕裂。**不**宣称解决磁盘永久损坏。
