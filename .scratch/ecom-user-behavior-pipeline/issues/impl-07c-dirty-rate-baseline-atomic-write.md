# impl-07c: 脏率基线落盘原子化 —— 读者永不看到半截文件

Type: task
Status: resolved
Blocked by: None（可立即开工）

**What to build:** 脏率基线文件的落盘改成**原子替换**——读者在任意时刻看到的要么是完整的旧版、
要么是完整的新版，**永不**看到被截断的半截 JSON。

## 背景：闸门文件却不是原子写的

基线文件由校验进程写入。今天的写法是**先截断再写**：

```
把文件清空 → 写新内容
```

如果写的过程被中断（Ctrl-C、进程被杀、机器掉电），文件会**停在半截**——读者随后拿到的是
一段不完整的 JSON，只能报「文件不可解析」。

这个文件是**闸门文件**：面板的基线行、以及读基线的判据都依赖它。基线还是**自我维护**的
（首跑落盘、之后每次运行前推），意味着**它被写的频率很高**——不是「一年写一次的风险」，
是常规路径上的常规写入。

窗口本身不宽（文件只有一百字节上下），但两件事叠加让风险实质化：

1. 写入与读取**可能同时发生**——自维护基线的机制里，读（校验判据读它）和写（校验判据
   写它）本就是同一进程的前后脚；
2. 读侧今天把解析错误一律报成「基线文件不可解析」⇒ 一次半截文件会让**所有**后续
   面板同步与脏率判据一起报同一个误导性错误。

⇒ 改法：**先写临时文件，再原子替换**（同目录写临时文件 → 替换目标文件）。替换是原子的，
读者只会看到完整旧版或完整新版。

## Acceptance criteria

- [x] 基线落盘改为**先写临时文件再原子替换**，目标文件在任意时刻都是完整 JSON
      ⇒ `save_dirty_rate_baseline` 改走同目录 `NamedTemporaryFile` + `os.replace`（Answer §1）
- [x] 临时文件与目标文件**同目录**（跨文件系统替换不原子）；临时文件在成功替换后**不残留**
      （异常路径也不残留）
      ⇒ 守卫 `test_temp_file_is_created_in_the_same_dir_as_the_target` 钉同目录；
      `finally` 清异常路径，另加**硬杀残骸清扫**（SIGKILL 跑不了 `finally`，见 Answer §3 与 code-review 处置）
- [x] **反例凭证**：在写入中途打断进程（构造一个写到一半就退出的场景）⇒ 目标文件**仍是
      完整的旧版**（不是半截），且下一次读它**能正常解析**
      ⇒ `evidence/impl-07c/02-interrupt-during-write.out.txt`：**真进程 + SIGSTOP 自停 + SIGKILL**，
      两臂对照——施工前留下 25 字节半截（读侧 `None`），施工后目标是 51 字节完整旧版（读侧正常解析）
- [x] **正例凭证**：正常写入后，文件内容与写入前的非原子写法**逐字一致**（同样的字段、
      同样的格式、同样的换行）——本票不改变基线文件的**格式**，只改变**怎么落盘**
      ⇒ `evidence/impl-07c/01-format-identical-to-head.out.txt`：把 `git show HEAD:` 的
      旧函数取出来跑同一组输入，`cmp` 字节全等（51 B，`{'{\n  "dlq_offset": 14911,…}\n'`）
- [x] 首次落盘（目标文件不存在）与覆盖落盘（目标文件已存在）两条路径都验过
      ⇒ `test_first_write_creates_baseline_even_when_parent_dir_is_missing` /
      `test_overwrite_replaces_existing_baseline_with_new_values`；证据脚本两条都跑
- [x] 基线文件的**读者侧不需要改动**也能正常工作（读者拿到的永远是完整 JSON）
      ⇒ `load_dirty_rate_baseline` 与 `docker/scripts/superset-dirty-rate-sync.sh` **零改动**；
      另修掉 code-review 实测出的**权限位漂移**（0600 → 读者丢读权限），见 Answer §3
- [x] 新增守卫测试钉住「不得用先截断再写的方式落盘」；全量 pytest 仍全绿
      ⇒ `tests/test_verify_baseline_atomic.py` **21 条**（含**行为**守卫：钩住所有写模式入口
      + POSIX inode 变更）；`evidence/impl-07c/03-guard-mutation-proof.out.txt` 用三处变异
      证明守卫敏感（第一版黑名单守卫被 code-review 打穿，已重做）
- [x] 落盘失败（如目录不可写）仍按今天的语义抛出，由调用方兜底——**不得**静默吞掉
      ⇒ `test_temp_file_creation_failure_propagates_oserror` /
      `test_replace_failure_without_old_file_leaves_nothing_behind`（三处调用点
      `verify_runner.py:799,824,843` 的 `except OSError` 逐字未动）

## Notes

- **只改落盘方式，不改基线的字段、格式、写入时机与棘轮语义**。棘轮语义（首跑 SKIP 落盘、
  PASS 前推、收尾吸收）是 impl-06h 裁决 A 的落点，**本票不动**。
- 与 [impl-07a](impl-07a-dirty-rate-baseline-parse-true-cause.md) 互补：07a 保证
  「万一还是坏了，报真因」；本票保证「根本不会坏」。两张都值得做——本票是治本，07a 是兜底。
- 本票改的是 **Python 侧**（校验进程的基线写入），与 07a/07b 的**脚本侧**是不同面，
  三票互不阻塞。
- 临时文件命名要有仓库内一致性（不要用会撞名的固定名）；同目录下多个进程并发写基线的
  场景当前不存在，但临时名带上进程号更稳妥。
- 执行面：Python 侧改动可在宿主直接跑单测；实机基线复跑需 WSL。

## Answer（2026-10-27）

**结论：`save_dirty_rate_baseline` 从「先截断再写」改成「同目录临时文件 + `os.replace`
原子替换」——目标文件在任意时刻都是完整 JSON。落盘内容与施工前**逐字一致**（51 字节、
同字段、同缩进、同换行），读者侧零改动，三处调用点与 `except OSError` 语义逐字未动。
新增 21 条守卫；全量 pytest **390 passed**、lint/format/typecheck 全 PASS。**

### §1 改动

| 位置 | 施工前 | 施工后 |
| --- | --- | --- |
| 落盘 | `path.write_text(...)`（`open("w")` 一开就清零目标） | 同目录 `NamedTemporaryFile` → `flush` + `fsync` → `os.chmod` → `os.replace` |
| 临时名 | — | `.{DIRTY_RATE_BASELINE}.{pid}.{随机}.tmp`（带进程号，非同进程撞不上） |
| 异常 | 目标停在半截 | `finally` 清临时文件；**目标一个字节都没被碰过** |
| 硬杀 | 目标停在半截、无残骸概念 | 目标仍是完整旧版；临时文件残留由**下一次落盘前**的超龄清扫带走 |
| 权限位 | `0o666 & ~umask`（本仓 **0644**） | 显式补回同一语义（见 §3） |

**为什么 `fsync` 值得**：不加它，掉电后可能出现「替换生效了、内容还没落盘」⇒ 一个合法的
**空/半文件**，那也是一种半截。**边界如实写进 docstring**：本函数**不** fsync 父目录，故它
保证的是「内容先落盘」，不是「整套替换在掉电后一定持久」。对 60 字节、每次 verify 都重写的
基线，这个强度足够，不值得再加一次目录 fsync（code-review 指出原措辞是**overclaim**，已改）。

### §2 正例凭证：与 HEAD 逐字一致（不是自证）

[`evidence/impl-07c/01-format-identical-to-head.sh`](../evidence/impl-07c/01-format-identical-to-head.sh)
把 `git show HEAD:` 的**施工前那份函数**取出来、与工作树的新实现各跑同一组输入，然后比字节：

```
施工前（HEAD）落盘字节：b'{\n  "dlq_offset": 14911,\n  "input_events": 77700\n}\n'
施工后（原子）落盘字节：b'{\n  "dlq_offset": 14911,\n  "input_events": 77700\n}\n'
字节数：old=51 new=51
首次落盘 = 覆盖落盘 = HEAD 逐字：True
目录内容：['dirty-rate-baseline.json']   ← 两条路径都没有临时文件残留
读侧解析：DirtyRateSample(dlq_offset=14911, input_events=77700)
```

对照组是**真从 git 取的旧代码**，不是拿 `json.dumps` 复算一遍（那样断言就成了用实现证明实现）。

### §3 反例凭证：真进程、真信号、真文件系统

[`evidence/impl-07c/02-interrupt-during-write.sh`](../evidence/impl-07c/02-interrupt-during-write.sh)
让子进程各自在自己的**中断窗口内 `SIGSTOP` 自停**，父进程 `waitpid` 确认到达该点后
`SIGKILL`。两臂对照：

| 臂 | kill 时刻的目标文件 | 读侧看到 |
| --- | --- | --- |
| **A 施工前**（HEAD 形态，先截断再写） | **25 字节**：`{\n  "dlq_offset": 15555,\n` | `None`（不可解析）← **缺陷复现** |
| **B 施工后**（临时文件 + replace） | **51 字节**：逐字未变的完整旧版 | `DirtyRateSample(14911, 77700)` ← 正常解析 |

同一时刻 B 的目录里确实躺着一个临时文件（SIGKILL 跑不了 `finally`）——**这条是 code-review
的 Spec 轴抓到的真缺陷**，处置见 §4。脚本随后把这个临时文件的 mtime 拨到超龄，再落一次盘：

```
清扫前临时文件：['.dirty-rate-baseline.json.35167.mxsd0o0c.tmp']
清扫后目录内容（不计 arm.err）：['dirty-rate-baseline.json']
✓ 硬杀残留的临时文件在下一次落盘前被清扫
```

### §4 code-review 两轴处置

两轴并行（Standards / Spec）。**Standards：零硬违规**（HLD §3.3.1 导入序、§3.3.5 归属、
§3.3.6 门禁归属、ADR-0001 全部未破；ruff check/format PASS、basedpyright errors=0）。
**Spec：两处 AC 判为部分成立 + 一处守卫太弱**，全部已修并各留钉子：

| 发现 | 判定 | 处置 |
| --- | --- | --- |
| **S1（真缺陷）** 权限位漂移：`NamedTemporaryFile` 固定 **0600**，`os.replace` 连权限一起搬 ⇒ 首次落盘把基线从 **0644** 悄悄降成 0600，本来读得到它的读者（面板同步，可能另一个 uid）**丢掉读权限** | 违反「读者侧零改动」这条 AC | 替换前 `os.chmod` 到「旧文件原本的权限位」；首次落盘补回 `0o666 & ~umask`。守卫 `test_first_write_mode_matches_what_open_write_would_have_produced` / `test_overwrite_preserves_the_existing_mode`；实测凭证 [`04-permission-mode.out.txt`](../evidence/impl-07c/04-permission-mode.out.txt)（`0644 / 0644 / 0640 沿用`） |
| **S2（真缺陷）** 硬杀残骸：`try/finally` **抓不住 SIGKILL**，而票面 AC 写的是「异常路径也不残留」。施工版 docstring 却声称「异常路径把临时文件清掉，不留残骸」——对**本票存在的那个场景**是假话 | 措辞与事实不符 | 加 `_sweep_stale_baseline_temps`：每次落盘前清**超龄**（`STALE_BASELINE_TEMP_GRACE_S = 60 s`，设计值）的同前缀临时文件；docstring 改成分三种死法如实陈述。反例保护 `test_fresh_temp_of_a_concurrent_writer_is_not_swept`（新鲜文件不得误删，否则会踩并发写者） |
| **S3（真缺陷，守卫太弱）** 第一版守卫读函数源码做字符串黑名单，被**三处变异打穿**：① `with path.open("w")`（只查 `open(path`）② 截断挪进辅助函数 ③ `os.replace` 包进 `except OSError: pass`（黑名单不管异常语义） | 守卫做不了它声明要做的活 | 守卫下沉到**可观察行为**：钩住 `builtins.open` / `Path.open` / `write_text` / `write_bytes` / `os.open`，断言目标文件**从未**被写模式打开；POSIX 上再钉「覆盖落盘**换了 inode**」（原地重写不会）。凭证 [`03-guard-mutation-proof.out.txt`](../evidence/impl-07c/03-guard-mutation-proof.out.txt) 逐条跑三处变异，**全部必红**，未变异时绿 |
| **S4（真缺陷，弱断言）** `assert "OSError" in body` 命中的是 docstring 里的散文 ⇒ 拿注释证明行为 | 同义反复 | 删掉；契约由 `pytest.raises(OSError)` 的两条行为测试钉住（它们真让 `OSError` 穿过写盘函数） |
| **J1（判断项，Standards）** 票面 rationale 在函数 docstring 与测试文件头近逐字重复 | 保留 | 两处读者不同：函数 docstring 给改代码的人，测试头给改测试的人。删任何一处都会让另一处的读者失去上下文 |
| **J2（已处置）** `tests:206` 直接赋 `json.dumps` + 手写 try/finally 还原；5 处 patch `verify_runner.os` 实际 patch 的是**进程全局** `os` | 已改 | 统一 `monkeypatch.setattr(json, "dumps", …)` / `monkeypatch.setattr(os, "replace", …)`（诚实指向真正的全局对象） |
| **J3（已处置）** `assert "tempfile" in body` 钉的是实现细节 | 已收敛 | 只留 `os.replace(` 一条 + 剥掉 docstring 的代码段黑名单，并在 docstring 里写明「换成别的原子手段时删掉本条不损失覆盖」——**命题**由行为守卫兜 |
| **J4（判断项）** docstring 41 行 / 函数体 22 行 | 保留 | 本仓 `<h2>` 写法是既有风格（`verify_runner.py:714,756,773`；`fault.py:389+`）。但按 code-review 指出的 overclaim，§1 那条**边界声明**是新增的必要内容 |
| **J5（判断项，未改）** 目标 `chmod 0444` 时：旧 `write_text` 抛 `PermissionError`，新实现**成功**（rename 只需目录写权限） | 留档如实登记 | 票面 AC 括注的例子是「目录不可写」——那条语义**逐字保留**（守卫覆盖）。「目标文件只读」这种情形下新实现更宽容，且**不破**任何判据：基线是自我维护产物，能被原子替换掉是期望行为而非缺陷。若将来有人要求「目标只读时必须失败」，需显式加判据 |
| **J6（判断项）** `try/finally` + `temp_path = None` 哨兵，是本仓 `src/ecomgen/` 唯一一处 | 保留 | 必要性由测试钉着（`KeyboardInterrupt` 与 `SystemExit` 都不被 `except OSError` 抓住），不是投机抽象 |

### §5 守卫测试清单（`tests/test_verify_baseline_atomic.py`，21 条）

- **格式零变化** 2 条：手写期望字面量 + 与旧写法复刻的**字节级**对照（`read_bytes`，把换行也钉住）；
- **两条落盘路径** 2 条：首次（父目录都不存在）/ 覆盖；
- **读者侧** 1 条：两种路径落完都立刻解析得出原值；
- **反例（异常）** 3 条：替换前 `KeyboardInterrupt`、序列化阶段失败、首次落盘 `SystemExit`（此时目标**不存在**而非半截）；
- **临时文件** 3 条：同目录、名带进程号且两次不同、反复覆盖后零残留；
- **失败不吞** 2 条：建临时文件失败 / 替换失败都抛 `OSError`；
- **行为守卫** 2 条：目标从未被写模式打开、覆盖落盘换了 inode；
- **形态守卫** 1 条：剥掉 docstring 的代码段里不得出现截断形态；
- **权限位** 2 条：首次落盘 = `open("w")` 的产物、覆盖落盘沿旧权限位；
- **硬杀残骸** 3 条：超龄残骸被清扫、新鲜残骸不被误删、残骸在位不影响读侧。

### §6 范围纪律（票面 Notes 逐条对照）

- **只改落盘方式**：字段、格式、换行、写入时机、棘轮语义（首跑 SKIP 落盘 / PASS 前推 /
  `--corrupt` 收尾吸收 = impl-06h 裁决 A）**逐字未动**；
- **三处调用点未动**：`check_dlq_rate_not_increased`（`:824,843`）与
  `absorb_fixture_dirty`（`:799`）的 `except OSError` 语义逐字保留；
- **读者侧零改动**：`load_dirty_rate_baseline` 与 `docker/scripts/superset-dirty-rate-sync.sh`
  一字未动（票面 AC）；
- **不与 07a/07b 混流**：那两张是**脚本侧**（shell），本票是 **Python 侧**，三票互不阻塞；
- **不改口径**：基线由谁写归 impl-06h 裁决 A，本票未碰。

### §7 已知边界（如实登记）

- **未 fsync 父目录**：故「掉电后整套替换一定持久」**不**被本票保证；被保证的是
  「读者永远不会看到半截文件」（§3 的反例凭证覆盖的是这个命题）。
- **超龄清扫是「下一次落盘时」生效**，不是即时：硬杀残骸会活到下一次 `make verify` 落盘为止。
  这是有意的取舍——即时清扫需要一个常驻守护进程，而基线目录里多一个 60 字节文件不影响任何读者。
- **`STALE_BASELINE_TEMP_GRACE_S = 60 s` 是设计值**，非官方保证；它只需要大于「一次落盘的
  墙钟时长」（毫秒级），留 60 s 是为了任何并发写者都不会被误伤。

### §8 收口

- 票面 AC 8 条全部勾选，`Status: resolved`；
- [impl-dag](impl-dag.md) 07c 行与前沿指针已同步（07c 结案、07 系列三张副票全结）；
- 母票 [impl-07](impl-07-panel-baseline-parse-and-interpreter.md) 的 AC「三张副票全部收口」
  与「07b/07c 仍 open」这半句已同步；
- 全量 pytest **390 passed**（原 369 + 21 新）、`ruff check` / `ruff format --check` PASS、
  `basedpyright` errors=0（6 条 warning 全在 `src/ecomgen/__init__.py`，本票未碰）。

