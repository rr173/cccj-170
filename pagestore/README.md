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
  crash.py          稳定故障注入点
  store.py          缓存预算、事务、提交/中止、open/create
  cli.py            命令行演练
bin/ps.py           隔离模式入口（进程级演练）
tests/              进程驱动测试、独立参考模型、差分/损坏/再崩溃测试
FORMAT.md           持久格式与恢复协议的完整说明
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
再崩溃矩阵。压力数据由参数在运行时生成，仓库只保留小夹具、生成器与
有界验证结果，不含巨型数据库或日志。

## 独立参考模型

`tests/reference.py` 只消费公开事务操作流和“已持久决定”，自己重算每槽
期望值，**不 import 引擎的 WAL/页/恢复代码**。差分与崩溃测试逐槽比对
两个独立实现；持久边界由外部故障标记与明确的 fsync 完成信号核实。

## 故障模型边界

支持进程崩溃及本文档（见 `FORMAT.md` 第 1 节）描述的持久化模型：
fsync 被正确履行、单次页原子发布不撕裂。**不**宣称解决磁盘永久损坏。
