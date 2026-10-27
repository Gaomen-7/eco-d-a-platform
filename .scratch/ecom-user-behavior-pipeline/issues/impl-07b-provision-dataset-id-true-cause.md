# impl-07b: 置备脚本 —— dataset id 解析失败报真因

Type: task
Status: resolved
Blocked by: None（可立即开工）

**What to build:** 看板置备脚本在解析目标 dataset 标识符失败时，要区分「接口里确实没有这个
dataset」和「我根本没拿到可解析的内容」——后者今天也被报成前者。

## 背景：三种真因被压成一句话

置备脚本向看板接口取 dataset 列表、在其中找目标表那一行，取它的标识符。取的时候用了一段
内联的解释器代码，**该代码把「没找到」和「解析炸了」都表达成同一个结果：空**。于是脚本报
「找不到 kpi_current 的 dataset id」。

但实际有三种真因：

| 真因 | 现象 | 今天报成 |
| --- | --- | --- |
| 接口确实没有这个 dataset | 列表里没有目标表 | 找不到 dataset id |
| 接口返回的结构变了 | 解析代码的取值路径对不上，抛异常 | 找不到 dataset id |
| 响应体不是 JSON / 解释器不可用 | 解析直接失败 | 找不到 dataset id |

第三类尤其恶劣：**接口正常、目标 dataset 存在**，脚本却报「找不到」。
排查方向会被引向「看板里是不是没这个 dataset / 置备是不是没跑」，而真因在执行面。

与 [impl-07a](impl-07a-dirty-rate-baseline-parse-true-cause.md) 同一病根，但**输入源不同**：
基线脚本从文件读，这段从 stdin（HTTP 响应）读；取的键也不同（数值 vs 对象里的标识符）。
⇒ **两段共用不上同一个辅助**，本票不并入 07a。

## Acceptance criteria

- [x] 解析失败时能区分「目标 dataset 确实不在列表里」与「没能从响应里解析出目标」两大类，
      报错各自可据以行动 ⇒ 六类机读标签分流（见 Answer §2）
- [x] **反例凭证**：让响应体不是合法 JSON（或让取值路径对不上）⇒ 脚本必须报「没能解析出
      目标」这一类，**不得**报「找不到 dataset id」
      ⇒ 故障注入 ①–⑦（HTML 登录页 / 截断 JSON / result 非数组 / 缺 id 键 / id=null /
      接口回 message / curl 退 7）全部 PASS，无一报「找不到」
- [x] **真·找不到的凭证**：让接口返回一个不含目标表的合法列表 ⇒ 脚本必须报「找不到」，
      这一类**不得**被新逻辑吞掉或改写成别的说法
      ⇒ 故障注入 ⑧ 报「找不到」，其余七类**均不得**报这句（守卫测试逐条断言）
- [x] 目标 dataset **存在且一切正常**时，解析结果与施工前**逐字一致**（同 id）、
      退出码一致、后续步骤无变化 ⇒ 真栈新旧脚本对照：同 id=1、stdout 逐字、退出码 0/0、
      chart_data 仍 success（`evidence/impl-07b/01`）
- [x] 报错带**可操作的下一步**（该查接口返回什么 / 该重跑置备 / 该看解释器是否可用）
      ⇒ 每一臂都带；守卫测试断言「下一步」关键词 + 具体动作都在
- [x] 新增守卫测试钉住「解析失败与未找到必须是两种不同的失败」；全量 pytest 仍全绿
      ⇒ `tests/test_superset_provision_dataset_id.py` 13 条；全量 366 passed
- [x] 脚本语法自检通过；**正常路径输出与施工前逐字一致**（除错误分支外零变化）
      ⇒ `bash -n` OK；真栈 stdout/stderr/退出码三面对照全 PASS

## Notes

- **只碰报错分支**：不改置备流程、不改 zip 导入次序（置备必须先于导入，那条顺序是承重的）、
  不改匿名可达断言、不改脏率同步调用点。
- 本票**不改口径**（基线由谁写），那是 impl-06h 已结案的裁决 A。
- 与基线同步脚本侧的面归 [impl-07a](impl-07a-dirty-rate-baseline-parse-true-cause.md)；
  两者**刻意不合流**——输入源与语义不同，合流会造出一个谁都不合适的抽象。
- 守卫测试的落点可参考现有的面板交付面静态守卫测试文件（同样的读文本、不连 docker 风格）。
- 执行面：脚本在 WSL 下跑（宿主无 `make`）；`docker` 命令需 `MSYS_NO_PATHCONV=1`。

## Answer（2026-10-27）

**结论：解析段改成「先把响应体落盘、再把 JSON 映射成机读标签、最后按标签分流」——
「接口里确实没有」「结构变了」「没能解析出目标」三类各自报真因、各自带下一步；
**唯一**报「找不到」的是「合法列表里确实没有 kpi_current」那一类，且它有反例保护不被吞。
正常路径与施工前**逐字一致**（同 id=1、stdout 逐字、退出码 0）。未改置备流程、未改 zip 导入
次序、未改匿名可达断言、未改脏率同步调用点。全量 pytest 366 passed、lint/format/typecheck 全 PASS。**

### §1 改动（只碰解析段的报错分支）

| 位置 | 施工前 | 施工后 |
| --- | --- | --- |
| 取响应 | `curl … \| python -c …`（管道） | 先 `curl -o "$DS_BODY" -w '%{http_code}'` **落盘**，再解析文件 |
| 解析 | `try: … except StopIteration: pass` ⇒ 一切失败都是「空」 | heredoc 解析器，每类失败吐一条**机读标签**（`OK / NOT_FOUND / BAD_JSON / READ_ERROR / API_ERROR / STRUCT`） |
| 分流 | `[ -n "$DS_ID" ] \|\| fail "找不到…"` | `case` 六臂，各报各的真因 + 各带下一步 |
| 解析器缺席 | 混进「找不到」 | 单独一臂（rc=126/127）报「解析器无法执行」 |

**为什么先落盘再解析**：管道形态下 **curl 的失败**与**解释器的失败**都表现为「一个空串」——
两者再被同一句「找不到」收口，正是本票要消灭的误诊载体。落盘后「取不到响应」与「取到了但
解析不了」在结构上就不可能混为一谈。

### §2 分流表（每一类都带下一步）

| 机读标签 | 触发 | 报错关键句 |
| --- | --- | --- |
| `OK` | 列表里有 `table_name=kpi_current` 的行 | （正常路径，不打印错误） |
| `NOT_FOUND` | 合法列表里**没有**该行 | 「找不到 kpi_current 的 dataset id（…其中没有 table_name=kpi_current 的那一行）——**接口是通的、响应是合法 JSON，是这个 dataset 确实不在接口返回的列表里**」 |
| `BAD_JSON` | 响应不是合法 JSON（HTML 登录页 / 截断 / 空） | 「**没能从 dataset 列表响应里解析出目标**：响应体不是合法 JSON（…）——**这不是『接口里没有这个 dataset』**」 |
| `STRUCT` | 顶层非对象 / 无 `result` 键 / `result` 非数组 / 行非对象 / 行缺 `table_name` / 目标行缺 `id` / `id` 不可用 | 「dataset 列表响应的**结构变了**：<点名哪一处，并把实际键名列出来>」 |
| `API_ERROR` | 响应里是 `{"message": …}` | 「dataset 列表接口回了 error message：…」 |
| `READ_ERROR` | 落盘文件读不出 | 「没能读出 dataset 列表的响应体：…」 |
| （无标签）rc=126/127 | 解析器不在 PATH / 不可执行 | 「解析器无法执行：…（bash rc=127）」 |
| （无标签）其他 rc | 解析器崩了但没吐标签 | 「没能从 … 解析出目标：解析没有任何机读输出（rc=…）」+ **原样带出解释器 stderr** |
| （curl 非 0） | 接口连不上 | 「没能取回 dataset 列表响应：curl 退出码 N」 |

### §3 正例凭证：正常路径逐字零变化（真栈）

[`evidence/impl-07b/01-normal-path.sh`](../evidence/impl-07b/01-normal-path.sh) 把 `git HEAD:` 上的旧脚本取到
临时文件，与新脚本在**同一套真实栈**（真 Superset 容器、真 dataset、真 chart_data）上各跑一遍：

- **id 一致**：旧解析形态取到 `1`，新脚本 `✓ kpi_current id=1`；
- **stdout 逐字一致**（`diff` 空）；
- **stderr**：脚本自有行（`[superset-provision]` 前缀）两边都是 0 行、逐字一致；去掉两次运行各自的
  容器日志时戳后整份也逐字一致（时戳差异与脚本行为无关）；
- **退出码一致**：0 / 0；
- **后续步骤无变化**：用取到的 id 打 `chart_data` 仍 `status=success`。

（导出 zip 指到临时目录：真脚本会刷新**已入库**的 `docker/superset/export/ecom-live.zip`，凭证不碰工作树里的入库产物。）

### §4 反例凭证：故障注入（真栈，[`02-fault-injection.out.txt`](../evidence/impl-07b/02-fault-injection.out.txt)）

手法：把 `curl` 前置到 PATH，桩**只劫持 dataset 列表那一个 URL**、其余原样转发给真 curl ⇒
pymysql 检查 / ORM 置备 / 匿名断言 / zip 往返 / 脏率同步全走真栈，注入面只剩解析段。
第 ⑧ 类**连 curl 都不换**：真接口、合法响应、dataset 确实存在，只把解析器换成不存在的路径。

| # | 注入 | 判定 |
| --- | --- | --- |
| ① | 响应是 HTML 登录页 | 报「没能解析出目标」（**PASS：未报「找不到」**） |
| ② | 响应被截断（半截 JSON） | 报「没能解析出目标」（PASS） |
| ③ | `result` 不是数组 | 报「结构变了」（PASS） |
| ④ | 目标行没有 `id` 键 | 报「结构变了：kpi_current 那一行没有 id 键（行内键：table_name,uid）」（PASS） |
| ⑤ | `id` 是 `null` | 报「结构变了：id 不可用」，**且没有继续用这个 id 打 chart_data**（PASS） |
| ⑥ | 接口回 `{"message": "Forbidden"}` | 报「接口在报错」（PASS） |
| ⑦ | curl 退出码 7（连不上） | 报「没能取回响应」（PASS） |
| ⑧ | 真接口 + 解析器不在 PATH | 报「解析器无法执行（rc=127）——**这不是『接口里没有这个 dataset』**」（PASS） |
| ⑨ | **真·找不到**（合法列表没有 kpi_current） | 报「找不到」，且**不得**被改写成解析失败（PASS：反向保护） |

★ 第 ⑧ 行就是票面说的「最恶劣」那一类：**接口正常、目标 dataset 存在**，施工前会报
「找不到 kpi_current 的 dataset id」，施工后报解析器问题——排查方向不再被引向看板。

### §5 新增守卫测试（`tests/test_superset_provision_dataset_id.py`，13 条）

假 PATH 上的 `curl` / `docker` 桩，不连 docker、不进 Superset（与 `test_superset_delivery.py` 同风格）：

- **正常路径**两条：取到真 id（逐行手写期望）+ **与 `git HEAD:` 旧脚本逐字对照**（真对照，不是自证）；
- **三类互不冒充**：真·找不到唯一报「找不到」＋ 非 JSON / 截断 / 接口报错 / 结构变 / 缺 id 键 /
  id 不可用 / 连不上 / 解析器缺席 **各自**不得报「找不到」；
- **每条报错都带下一步**（`下一步` 关键词 + 具体动作）；
- **静态形态**：全脚本不得再出现 `except StopIteration`（只看非注释行）、不得再用
  `python -c` 吃管道、必须先把响应体落进 `DS_BODY`、三类标签必须在位、`MSYS_NO_PATHCONV=1` 不得丢。

### §6 范围纪律（票面 Notes 逐条对照）

- **不改置备流程**：pymysql 检查 / `superset-provision.py` / 匿名可达断言三段逐字未动；
- **不改 zip 导入次序**：「置备 → 导出 → 导入回读」顺序逐字未动（那条顺序承重）；
- **不改匿名可达断言**：三条 curl 断言逐字未动；
- **不改脏率同步调用点**：⑤ 段逐字未动；
- **不改口径**：基线由谁写归 impl-06h 裁决 A，本票未碰；
- **不与 07a 合流**：这正是碎成两张票的理由——那边从**文件**读、取两个数值键；这边从
  **接口响应**读、取一个对象里的标识符。本票的实现（落盘 + 标签分流）复用不到 07a 的
  解释器自检/字段校验那套，硬合流只会造出一个两个调用点都不合适的抽象。

### §7 收口

- 票面 AC 7 条全部勾选，`Status: resolved`；
- [impl-dag](impl-dag.md) 副票行与前沿指针已同步（07b 结案、前沿剩 07c）；
- 执行面：脚本与凭证都在 WSL 下跑（宿主无 `make`）；`docker` 命令需 `MSYS_NO_PATHCONV=1`。


### §8 code-review 两轴处置

两轴并行复核完毕（Standards / Spec）。**Standards 轴无硬违规**（ruff check / format --check
全 PASS；术语与 GLOSSARY 一致；本仓 shell 无 lint 门禁）。Spec 轴抓到 **三个真缺陷**，
全部已修并各加回归钉：

| 发现 | 处置 |
| --- | --- |
| **S1（真缺陷，Spec ⑥）** 解析器对**任意一行**缺 `table_name` 就报 STRUCT 致命；旧代码是 `r.get("table_name")` 对缺键行**跳过** ⇒ 目标行存在、另有一行缺键时，旧脚本成功、新脚本退 1，直接违反「正常时零变化」 | 改成 `row.get("table_name") != "kpi_current"` 即 `continue`（只有**目标行**结构不对才报）；新增钉 `test_other_rows_missing_table_name_key_are_skipped_like_before` |
| **S2（真缺陷，Spec ④）** `mktemp -d` 失败时 `DS_BODY` 会落到 `//dataset-list.json`，随后被读成「接口根本连不上」——给正常路径自造失败面且无人接住 | mktemp 后立刻判空 + 判目录，单报「建不了临时目录（磁盘满 / TMPDIR 不可写）」，不再流到 curl 那一臂 |
| **S3（真缺陷，Spec ⑤）** `id` 加了 `.strip()`，旧代码是原样 `str(r["id"])` ⇒ 带空白的 id 与「同 id 逐字一致」有出入 | 去掉 `.strip()`，只保留「是否空白」的**判据用途**；新增钉 `test_dataset_id_is_used_verbatim_without_stripping`（`" 0042 "` 原样用） |
| **S4（半程，Spec ①②）** 票面 L22 的「解释器**存在但不可用**（空壳）」只覆盖了「缺席 rc=127」；票面 L35-36 要求「取值路径不对」归入「没能解析出目标」，实现却自造了一个平行的「结构变了」 | ①`STRUCT` 各臂措辞改成**先**说「没能从 dataset 列表响应里解析出目标：响应的结构变了——…」，该大类回到统一前缀，守卫断言两条都要命中；②空壳解释器补一条守卫 + 一条真栈凭证（`[9b]`：rc=49、零输出、零 stderr ⇒ 报「解析没有任何机读输出…**存在但不可用的空壳解释器**正是这种形态」） |
| **S5（措辞，Spec ⑦）** STRUCT 的下一步写「改 superset-provision.sh **第②步**的解析段」，可解析段不在第②步（② 是 ORM 置备） | 改成「改本脚本「解析 kpi_current dataset id（不硬编码）」那一段的 heredoc」 |
| **T1（判断项，Standards）** 六个 fail 臂逐字复制「这不是『接口里没有这个 dataset』」模板 | 保留：这是**判据措辞的一致性**（每条都必须点明「不是什么」），抽成常量会让每条报错读起来像模板填空、失去指认具体真因的能力。`PROVISION_HINT` / `LIST_URL` / `PROBE_HINT` 三个**纯重复的下一步**已抽常量 |
| **T2（判断项，Standards）** 测试的 `_CURL_STUB` 与凭证脚本的 `CURLSTUB` 形态相近 | 保留：两者**服务的契约不同**（测试桩要能模拟各种坏响应；凭证桩要按真 curl 的 `-o`/`-w` 契约转发其余调用）。硬并成一处会把测试耦合到凭证脚本上 |
| **T3（判断项，Standards）** `emit()` 名字不体现终止语义 | 改名 `emit_and_stop()` 并加 docstring |
| **T4（已处置，Standards）** 测试里重复钉 `MSYS_NO_PATHCONV=1`（`test_superset_delivery.py` 已钉同一文件） | 删掉本文件的重复断言，留一行注释指回唯一属主 |
| **T5（已处置，Standards）** `import json as jsonlib` 是仓内唯一别名 | 改回 `import json` |

**复核后仍未处置（留待人工判断，非遗漏）**：
- 静态守卫断言的是**源码文本形态**（有 `NOT_FOUND`/`BAD_JSON`/`STRUCT` 标签、先落盘再解析）。
  它挡的是「回退到管道 + 折叠成空」这个**具体形态**，其判据由行为测试（六类互不冒充）兜底；
  若将来解析段换实现方式（例如抽成独立 `.py`），这条需要同步改。
- `pyproject` 的 `[tool.pyright] include=["src"]` 不含 `tests/`，故新测试的类型注解不受
  类型门禁约束——这是**全仓既有口径**，不是本票引入的。

### §9 端到端复跑（收口门）

bash docker/scripts/superset-provision.sh 在真栈上完整跑通：RC=0，末行 ✓ 完成，
✓ kpi_current id=1、✓ 匿名 chart_data 出数（链路活着）、✓ zip 导入回读成功、
✓ 脏率基线已同步 全绿。**注**：该次端到端复跑会刷新入库产物
docker/superset/export/ecom-live.zip（脚本行为，非本票改动），复跑后已把该文件还原成 HEAD 内容
——工作树 diff 仅剩本票的两处改动 + 两处新增。


### §10 收口门最终一轮（code-review 修复后复跑）

| 门 | 结果 |
| --- | --- |
| 全量 pytest | **369 passed**（新增 16 条守卫：原 13 + code-review 三颗回归钉） |
| lint / format / typecheck | ruff check PASS、ruff format --check「134 files already formatted」、basedpyright errors=0 |
| 脚本语法 | bash -n SYNTAX OK |
| 正常路径逐字零变化（真栈） | 同 id=1 / stdout 逐字 / stderr 脚本自有行逐字 / 退出码 0（\`evidence/impl-07b/01-normal-path.out.txt\`） |
| 故障注入（真栈） | ①–⑦ 全部归入「没能解析出目标」或更具体的一类、**无一报「找不到」**；⑧ 真·找不到报「找不到」；⑧/⑨b 解析器缺席与**空壳**各报各的（\`evidence/impl-07b/02-fault-injection.out.txt\`） |

★ **回归钉的三条来源**（都是 code-review 抓到的、我原本漏掉的真缺陷）：别行缺键必须跳过、
id 必须逐字使用（不 strip）、空壳解释器必须单列一臂并给出「先查解释器」的下一步。
票面 AC 第 4 条「目标 dataset 存在且一切正常时逐字一致」正是被第一条违反过的——
**没有这三颗钉，一条「看起来对」的实现就能蒙混过关**。

