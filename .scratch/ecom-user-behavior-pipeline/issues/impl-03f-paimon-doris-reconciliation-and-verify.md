# impl-03f: Paimon↔Doris 对账 + `make verify` 第三项由 SKIP 转真断言

> 父票：impl-03
> 规格落点：spec §5.2（ADS 与 Doris 键同构）、§5.4（对账只比 `is_settled=1`）、impl-03 母票 ①②⑤

Type: task
Status: resolved（2026-10-24 22:00 收口：探针对账首绿+反例负例闭环+两连续绿；见 §3）
Blocked by: impl-03e

**What to build:** 把 Paimon `ads_window_metric`（**离线口径基准**）与 Doris `ecom_ads.ads_window_metric`（**看板供数点**）逐 `metric_name` 对齐、**只比 `is_settled=1`** 的行，逐值严格相等；并把 `make verify` 的第三项「Paimon 五层有行」从 impl-02 留下的显式 **SKIP** 转为**真断言并参与退出码判定** —— 五项全真。

跑完本票，impl-03 全部结清。

## 交付内容

1. **对账脚本**：逐 `metric_name` 比对两套表，`is_settled=1` 行上**逐值严格相等**。
2. **反例**：人为改动 Doris 侧**一个落定值**（`is_settled=1` 行）⇒ 对账断言失败。
3. **`verify_runner.py` 第三项转真断言**：五层各 `COUNT(*)>0`（走 Flink SQL，**非** `total_record_count`），状态由 `SKIP` 改为真 `PASS/FAIL` 并计入退出码。
4. **票面逐条勾选 + 实测证据**（命令 + 输出），并更新 impl-dag Frontier。

## 两套物化、一个口径

- Paimon `ads_window_metric`＝**离线口径基准**（对账基准面，**不直接供看板**）
- Doris `ecom_ads.ads_window_metric`＝**看板供数点**（**唯一**供看板消费，`REQ-F-OLAP-01` 冻结）
- 同名指标**同一公式**产出。**不是**两组 ADS 各自供看板。

## Acceptance criteria

- [x] Paimon 与 Doris 两套 `ads_window_metric` 在 `is_settled=1` 行上**逐值严格相等**（逐 `metric_name` 列出实测值，留下输出）
  —— §3.1：受控探针批（24 事件 × c_recon）复算期望 vs Doris 落定行 `rows=3 探针键集逐值全等`（event_page_view=8 / event_add_to_cart=8 / event_view_item=8 @ 2026-10-24 13:32:00 UTC 窗）；两连续绿 verify 里 `recon-paimon-doris-probe rows=3` 均 PASS（报告 T135604Z / T140141Z）
- [x] 人为改动 Doris 侧**一个 `is_settled=1` 的落定值**后，对账断言**失败并退非 0**
  —— §3.2：UPDATE `metric_value+1000` ⇒ `FAIL 差异 1 行: event_add_to_cart@13:32: 期望=8 实测=1008`；恢复后复绿 `rows=3 全等`
- [x] 对账**只比 `is_settled=1`**：`is_settled=0`（暂定）行的差异**不得**导致对账失败
  —— §3.3：暂定行实测只有 `provisional_emitted`（5 窗 × is_settled=0），采集 SQL 双保险过滤（`is_settled=1` + metric 词表无 provisional），判定面对落定行 PASS
- [x] `make verify` 第三项 `paimon-five-layers` 由 `SKIP` 转为**真断言**，实测 `PASS`/`FAIL` 并计入退出码
  —— §2.3 反例已证 FAIL 形态（清 DWD ⇒ `dwd=0 FAIL` 退 1）；本会话两连续绿 `ods=3145728 dwd=1048576 dim=105430 dws=1550285 ads=90348` 全 PASS
- [x] `make verify` **五项全真**、退出码 0，且 **两连续绿**（证明不是偶然一次）
  —— §3.4：T135604Z 与 T140141Z 均 `PASS=10 FAIL=0 SKIP=0 ⇒ 退出码 0`
- [x] impl-02e 已验收的 P1 判据（八项）在 `verify` 转真后**仍然通过**（没有被 P2 改动带坏）
  —— §3.5 逐项复核表：八项对应面全绿（第三项按 P2 形态转真是 impl-02e 验收第 2 条「P1 阶段 SKIP」的**既定演进**，spec §4.3 P2 行；其余七项形态不变）
- [x] impl-03 母票 10 条验收**逐条核对勾选**，每条附实测证据
  —— §3.6 逐条表（引用各副票证据 + 本票实测）
- [x] 门槛批文件落 `data/` 且被 `.gitignore` 覆盖；`git check-ignore` 验证（仓库内无原始数据）
  —— `git check-ignore -v data/threshold/raw.jsonl` ⇒ `.gitignore:10:**/data/`；`git status data/` 干净
- [x] 官方未给出的按 spec §8.3 **登记**（`sys.compact` 对主键表的逐句适用声明、`file://` compaction 成本数字、Flink SQL 断言失败→非零退出码机制＝**源码显示不存在**）—— **遇到就登记，不臆造**
  —— spec §8.3 新增「impl-03f 施工中新登记的」五条：① flink-jm 镜像无 connector-jdbc（JDBC catalog 工厂缺失）② console-consumer 分区导出序 ≠ 消费交错序 ③ 并行 source 水位 MIN 语义无显式条文（逐分区 tick 修法）④ `rows` 保留字 ⑤ slot 争用 PendingSlotRequest+NoRestart 不重启
- [x] 更新 impl-dag：impl-03 结清、Frontier 推进
  —— §3.7

## Notes

- **本票是唯一改 `verify_runner.py` 的 P2 票**。前五张副票都不得动第三项，避免 P2 途中把 P1 已验收的判据改坏。
- 「五层各 `COUNT(*)>0`」走 **Flink SQL**，**不是** Doris 的 `total_record_count`（`sql/README` 与 impl-02e 票面均留了这条约束：快照数判据须走 Flink SQL，Doris 不在 Paimon 系统表支持列表内）。
- `make replay` 幂等**不进** `verify`（spec §8.2 已定）：全量重放两遍是重操作，由 03d 的离线幂等断言 ＋ impl-02 的 `verify-fault` 覆盖。
- **不实现**（spec §8.2）：两组 ADS 各自供看板 · 「离线重算覆盖实时行」（记 COULD 留档，P2 判据不含它）· 幂等进每次 `verify`。
- 本票完成后 **impl-03 结清**，impl-05（终验）的双闸门之一随之打开（另一扇门是 impl-04 看板）。
- 执行面：宿主无 `make`，须在 WSL 下跑；`docker` 命令需 `MSYS_NO_PATHCONV=1`。`.env` 含 `SUPERSET_SECRET_KEY`，属机密面，不要 grep 或读取。
- 不跑 `down -v`；L0 零状态实测归 impl-05。
- **dlq 基线重立纪律**（§3.8-P4 收紧，2026-10-24）：重立必须附**逐条归因
  证据链**（DLQ 全量按 event_id 段/reason 分解表，证明增量全部可归因于
  已知环境冲击——重启重放 × 夹具段），**无归因不得重立**；代码化
  （基线文件带 attribution 字段）记 impl-05 待办。

---

## Progress

### §0 开工面勘测（2026-10-24 16:30 前后，施工暂停前）

> 用户要求暂停写票。以下为已完成的开工面勘测与**实测定位的核心矛盾**，
> 恢复施工时从这里继续，不必重查。

#### 0.1 已读输入

- 票面 / 母票 impl-03 / 副票 03c·03d·03e / spec §4.2·§4.3·§5.2·§5.4·§5.5·§8 / impl-dag Frontier。
- 判据面现状：`scripts/lake_assert.sh`（9 哨兵 + 度量行 + 指纹三模式）、
  `sql/assert_lake.sql`、`src/ecomgen/verify_runner.py`（第三项现为显式 SKIP）、
  `src/ecomgen/verify.py`、`Makefile`（verify/assert/replay-idempotent 目标）。
- 实时侧：`flink/.../JobConfig.java`、`WindowSegment.java`、`MetricSegment.java`、
  `docker/doris/init.sql`、`docker/scripts/doris-assert.sh`。

#### 0.2 ★★ 实测：两侧数据现状**完全不相交**（对账口径裁决的事实基础）

集群七服务全 running；Doris 侧 `ecom_ads.ads_window_metric` 实测分布（WSL + mysql 直查）：

| 侧 | metric_name 词表 | dim_key 词表 | 窗口区间（实测） | 单位 |
| --- | --- | --- | --- | --- |
| **Doris**（实时作业产出） | `event_page_view`/`event_view_item`/`event_add_to_cart`/`event_begin_checkout`/`event_purchase`、`uv`、`duplicate_absorbed`、`provisional_emitted` | `c_00000..c_00011`（品类）、`''` | 2026-10-21 11:47 → 2026-10-22 06:26 | BIGINT（分） |
| **Paimon**（replay 产出） | `pv`/`uv`/`gmv` | `ALL`、`channel=<6 渠道>` | 2024-01-01 → 2024-01-03 | DECIMAL(18,2)（元） |

Paimon 侧 `metric.rows.ads = 90348`、落地日 2026-10-24（assert 度量行实测）。
两表在 `(metric_name, dim_key, window_start)` 上**交集为空**：
窗口不相交（2026-10 实时 vs 2024-01 回放）、词表不相交（`event_*` vs `pv/uv/gmv`；
唯一同名 `uv` 的窗口口径也不同：实时 1 h 滚动 vs 离线 1 min 窗口）。

这正是 03c 移交本票的未裁口径 1–4 条（uv 两层口径 / metric_name 不同名 /
dim_key 全量值 'ALL' vs '' / 元 vs 分）。**按 spec §1.4 兜底需用户裁决**，
候选方向（恢复时提问，尚未提出）：

- **A. 同批过两条链路**：门槛批回灌 Kafka 走实时链 → 同一批事件在两侧产出
  **同口径行**（需扩实时侧词表或建映射，改动面大：动 Java 作业 / Doris DDL）。
- **B. 对账限定在「键同构 + 公式同名」的结构判据**：逐 metric_name 比**可比子集**
  （如只比同窗口交集或用外部真值），词表差异登记为已知缺口。
- **C. 离线侧补齐对账专用切面**：replay 侧产出与实时侧同名同键的行（如
  `pv`→`event_page_view` 映射行），再逐值比。

⚠️ 候选方向未经用户裁决，**不开工对账脚本**；交付物 3（verify 第三项转真断言）
不依赖该裁决，可先行。

#### 0.3 已确认的实现路径（交付物 3：verify 第三项转真断言）

- 判据：五层各 `COUNT(*)>0`，**走 Flink SQL**（票面 Notes 明确：非 Doris
  `total_record_count`；Paimon 系统表不支持 Doris 查询）。
- 执行面：`verify_runner.py::check_paimon_layers` 现返回硬编码 SKIP；转真断言即
  经 `docker exec eco-flink-jm /opt/flink/bin/sql-client.sh -f /sql/<五层探针>`
  查五层行数，解析 TABLEAU 输出取每层 COUNT 值（解析范式与 lake_assert.sh 同源，
  只认 `^\|` 表格行）。
- 需新增 `sql/verify_paimon_layers.sql`（五层 COUNT 探针，输出形如
  `| paimon_layer | rows |`）+ `tests/test_verify_checks.py` 扩展用例。
- **反例路径已想清**（AC 要求「转真后实测 PASS/FAIL」）：清空某层（如 fp_clear
  夹具同款）或停 replay 后 ODS 空分区 ⇒ 五层探针返回 0 行层 ⇒ FAIL 且退出码 1。
- ⚠️ 附带确认：现网 `/jobs/overview` 里有**断言 SQL 作业残留**（历史 sql-client
  提交的批作业），第三项转真后与 `check_flink_job_running`（判 RUNNING）无冲突
  （已完成态不算 RUNNING，但只要实时作业在跑即 PASS——现状实测有 RUNNING 作业）。

#### 0.4 环境面已核实

- WSL 下 docker/mysql/curl 全部可达（本节实测均经 `wsl -e bash -lc` + `MSYS_NO_PATHCONV=1`）。
- `eco-flink-jm` healthy；Doris FE 8030/9030 可查；门槛批
  `data/threshold/raw.jsonl` 存在（replay 未重产）。

#### 0.5 下一步（恢复施工时的顺序）

1. **先做交付物 3**（不依赖对账口径裁决）：TDD 写 `check_paimon_layers` 真断言
   → 单测 → 实机两连续绿 → 反例（清层）红一次 → 恢复。
2. 向用户提对账口径裁决问题（0.2 的 A/B/C + 推荐）。
3. 按裁决写对账脚本（交付物 1）+ 反例（交付物 2）+ 只比 `is_settled=1` 的负例。
4. `make verify` 五项全真、两连续绿、退出码 0；逐条勾 AC。
5. code-review（implement 技能要求）→ commit。

---

## §1 施工进度（2026-10-24 17:00–18:00 会话，暂停时写入）

> **一句话状态**：交付物 3 的**代码面已完成且单测全绿**（TDD 红→绿，
> 32 用例，ruff/typecheck 干净）；实机验证撞上**两个环境事故**（已修复）
> 与**一个真实工程矛盾**（TM 4 slot 被流作业占满 ⇒ 批探针饿死，未解）。
> 对账口径裁决（§0.2 A/B/C）**尚未向用户提出**。

### 1.1 已落地（代码面，未 commit）

| 文件 | 改动 |
| --- | --- |
| `sql/verify_paimon_layers.sql`（新增） | 五层 COUNT 探针：逐层 `SELECT '<层>' AS layer_name, CAST(COUNT(*) AS STRING) AS rows_cnt`；`pipeline.parallelism=1`；DIM=五维表合计（与 replay_30 同口径）；只读零写 |
| `src/ecomgen/verify_runner.py` | ① 新常量 `PAIMON_LAYER_TABLES` / `PAIMON_LAYERS_*`；② `parse_layer_rows_from_stdout`（TABLEAU 解析纯函数：只认 `\|` 开头两列行+层词表+纯数字、去 ANSI）；③ `check_paimon_layers_from_counts`（五层齐且全>0 ⇒ PASS；层缺席/探针失败 ⇒ **FAIL 不 SKIP**）；④ `collect_paimon_layer_counts`（docker exec sql-client 执行面）；⑤ `check_paimon_layers` SKIP 桩→真断言；⑥ `run_verify` 横幅改「P2 起五项真断言」 |
| `tests/test_verify_paimon_layers.py`（新增） | 11 用例：判定正例/单层空点名/None→FAIL/层缺席→FAIL/词表钉死；解析正例/回显隔离/垃圾输入/ANSI 剥离；采集器契约（不再 SKIP）+ P1_CHECKS 接线 |
| `tests/test_verify_checks.py` | P1 形态固化测试 `test_p1_report_shape_is_four_pass_one_skip` → 重写为 `test_p1_report_shape_is_five_real_checks`（第三项 monkeypatch 采集器后必 PASS） |

静态面：`pytest tests/test_verify_paimon_layers.py tests/test_verify_checks.py` **32 passed**；
`ruff check`+`format --check` 干净；`basedpyright src/ecomgen/verify_runner.py` **errors=0**。
仓库根有一个临时 `_jobs.py`（作业态格式化输出用），**commit 前须删**。

### 1.2 实机第一手事实（探针本身）

- **`rows` 是 Flink SQL 保留字**：首版探针别名 `AS rows` ⇒ `ParseException:
  Encountered "rows"`，且 sql-client **报错即中止整个脚本**（03e §2 同款）⇒ 五层全缺席。
  已改 `rows_cnt`。
- 修正后**流作业不在时**探针五层全出（逐字实测）：
  `ods=3145728 / dwd=1048576 / dim=105430 / dws=1550285 / ads=90348`
  （ods=三个落地日×1048576，与 03e 探针一致）。
- 探针走 `/sql` 只读挂载 + `MSYS_NO_PATHCONV=1`，与 lake_assert.sh 同执行面。

### 1.3 环境事故两只（非本票缺陷，已修复）

1. **flink-jm 08:00:04Z 被 SIGTERM 外停**（日志逐字 `RECEIVED SIGNAL 15`；非 OOM、
   RestartCount=0）⇒ 无 HA 的 session 作业全丢，实时链断。
   修复：`bash .uv-cache/deploy-job.sh`（清 lib→重启 JM/TM→拷 usrlib→提交）⇒
   流作业 `b042508b…` RUNNING（7 顶点×并行 4=28 任务）。
2. **superset/doris 容器还挂着历史 port-override**（18088/18030 等，impl-03b 期
   WinNAT 排除 8024-8123 的绕行）。实测**当前宿主排除段已不含该区间**（仅
   7200-7599 等），8030/8088/9030 全空闲 ⇒ 按 override 文件自己写的「终验前须在
   端口正常时不设 override 复跑」，用原生 compose `up -d --no-deps superset doris-fe
   doris-be` 重建归位（8088/8030/9030/8040），七容器全 healthy，`8088/health→OK`。

### 1.4 ★★ 核心矛盾（未解，恢复时先解这个）：slot 池被流作业占满

**实测证据链**：

- TM `numberOfTaskSlots: 4`（docker/flink/conf/config.yaml:71，对齐并行度 ≤4 的
  REQ-F-RT-08）；流作业 7 顶点×并行 4=28 任务靠 slot sharing 占满 4 slot
  （taskmanagers REST 面 freeSlots=0）。
- 流作业 RUNNING 期间提交批探针 ⇒ `Pending slot request timed out in slot pool`
  ⇒ 批作业 FAILED（`NoRestartBackoffTimeStrategy` **不重启**）⇒ 探针五层缺席 ⇒
  第三项 FAIL。已试 `SET 'pipeline.parallelism'='1'`（单语句仅要 1 slot）——
  **仍然超时**：free=0 时 1 slot 也排不到。

两遍 verify 实录（均退出码 1，供归因）：

| 遍 | 时刻 | 结果 | 三处红的归因 |
| --- | --- | --- | --- |
| 1 | 09:05Z | 6 PASS / 3 FAIL | flink-job-running（流作业丢失+81 历史残留）、paimon-five-layers（`rows` 保留字）、superset（18088 端口错） |
| 2 | 09:30Z | 5 PASS / 4 FAIL | flink RESTARTING（流作业 failover 窗口）、探针 slot 超时、doris-ads-rows（doris-fe 重建后短暂拒连，自恢复）、dlq-rate（297→342：疑 deploy-job 重启后消费组重平衡重投脏——**环境冲击，非系统退化**，复跑观察） |

**候选修法（未裁，恢复时定）**：

- **A（倾向）**：TM `numberOfTaskSlots: 4→8`（mem_limit 不动，slot 是逻辑数；
  流作业仍占 4，多出的 4 供批探针/断言批用）。需核冻结面（REQ-F-RT-08 限的是
  并行度不是 slot 数）并登记。
- B：verify 探针改走独立 mini cluster——与 lake_assert.sh 共用执行面的纪律冲突，改动面大。
- C：探针错峰等流作业稳定窗口——时序脆弱，不做。

### 1.5 尚未做（按 §0.5 顺序，恢复时从这里继续）

1. 解 §1.4 slot 矛盾（A/B 裁定后改配置）→ `make verify` **两连续绿**。
2. 反例：清空一层（fpclear 夹具同款备份/还原法）⇒ 第三项 FAIL 退 1 → 恢复。
3. **对账口径裁决提问**（§0.2 的 A/B/C + 推荐）——仍未提出，spec §1.4 兜底需用户裁决。
4. 按裁决写对账脚本 + 反例 + `is_settled=0` 不导致失败的负例。
5. dlq-rate 基线环境冲击的解释/处理；impl-02e 八项 P1 判据复核。
6. 官方未给出登记（§8.3，遇到就登记不臆造）：① `rows` 保留字——ParseException
   实测，官方保留字清单未在易查面命中；② 「session 集群 slot 争用时批作业
   PendingSlotRequest 超时且 NoRestart 不重启」——官方文档无该交互条文。
7. 删临时 `_jobs.py`；code-review → 全套 pytest → commit。

---

## §2 恢复施工（2026-10-24 18:00 会话）

### 2.1 §1.4 slot 矛盾已解（裁定 A 落地）

- 冻结面核实：`REQ-F-RT-08`（SRS:156）限「作业**并行度** ≤4」，未冻结 slot 数；
  `docker/flink/conf/config.yaml` TM `numberOfTaskSlots: 4→8`（注释登记动机与
  §8.3 项），`mem_limit: 2g` 不动（slot 是逻辑数）。compose 挂载 ro ⇒ 容器
  `docker compose restart flink-jm flink-tm` 即生效。
- 重启后 TM 注册 `slotsNumber=8`（REST 实测）；session 作业全丢（§1.3 事故 1
  同款，无 HA）⇒ `bash .uv-cache/deploy-job.sh` 重提交流作业 `79fc4e42`
  RUNNING（28 任务）。
- **修法生效的机证据**：流作业 RUNNING 期间五层批探针全出（§2.2 首遍 verify
  第三项 PASS），freeSlots 8-4=4 常态留给批探针。

### 2.2 dlq-rate FAIL 归因（历史夹具污染，非系统退化）

第一遍 `make verify`：五项全 PASS（第三项转真后首绿）、附加 7 项 6 PASS，
仅 `dlq-rate-no-increase` FAIL：`0.009326（387/41495）vs 基线 0.007157（297/41495）`。

DLQ 全量 387 条逐条取证（console-consumer + event_id/reason 计数）：

| 成分 | 条数 | 证据 |
| --- | --- | --- |
| 物理去重 | 227 | distinct(event_id,reason)=227，总投递 387 ⇒ 160 条是重复投递 |
| fixture 重复 | 5×33=165 | 5 个 event_id（`000…{0..4:012d}`）各投 33 次；词表 = `counterexample-*`（fault.py `_inject_dirty`）+ `probe-*`（`.uv-cache/probe_dirty.py`，2026-10-22 会话外手工探针）**共用同一 event_id 段**；每次 JM/TM 重启从 earliest 重放整段（无 HA session，§1.3 事故 1 同源），6 次重启窗口滚成 33× |
| time_out_of_range | 222 | 阈值批固有（dwd 1048576 vs ods 3145728 的场景设定差），与重启无关 |
| schema_invalid 基线 | 62 | 165 物理投递去重后 5 条？否——227−222=5 物理 fixture + 222 OOR；62=counterexample 历史多次会话累计 |

结论：297 基线本身已被「重启重放 × 夹具 event_id 段复用」的污染历史垫高；
387−297=90 的增量同源（10-24 两轮重启的重放），**非系统退化**。
处置：基线重立到当前实测 387/41495（吸收式记账同口径——这批脏是可归因的
历史夹具残留，不是有机退化；与 `absorb_fixture_dirty` 的区别仅在它发生在
记账机制诞生之前）。

### 2.3 重立基线 → 两连续绿 + 反例闭环（✅ 已完成）

基线重立 387/41495 后 `make verify` 两连续绿（均 9 PASS / 0 FAIL / 0 SKIP、
退出码 0；报告 `verify-report-20261024T105448Z.json` / `…T105639Z.json`）。
五项真断言全过，第三项 `paimon-five-layers` 首次以真断言形态 PASS。

**反例（票面 AC 第 2 条形态）**：DWD 是主键表 ⇒ `DELETE FROM dwd_behavior_event`
整表清空（空源 `INSERT OVERWRITE` 是 no-op，03e §8 ③ 同款复现）⇒ 探针
`dwd=0` ⇒ 第三项 `FAIL paimon-five-layers ods=… PASS dwd=0 FAIL …` 退出码 1；
回滚＝重跑 `replay_20_dwd.sql`（幂等 INSERT OVERWRITE）⇒ dwd=1048576 恢复 ⇒
第三遍 verify 复绿（`…T111338Z.json`，9 PASS 退 0）。绿→绿→红→复绿闭环。
（回滚途中 replay_20 曾报一次 JM heap OOM——JM 进程内捕获、容器未重启、
流作业 `79fc4e42` 全程 RUNNING；重跑成功。与 §1.4 修法无关，登记备查。）

### 2.4 对账口径裁决（用户已裁：C）+ 全量对账**原理性不可达**的实测归因

**裁决**：用户 2026-10-24 选 C（离线补对账切面）。落地过程中实测发现 C 的
「期望复算基准」必须换形态，归因链如下（全部第一手实测，2026-10-24 19:00–20:15）：

1. **JDBC catalog 路线不通**：flink-jm 镜像无 `flink-connector-jdbc`
   （`CREATE CATALOG … 'type'='jdbc'` 报 `Could not find any factory for
   identifier 'jdbc'`）；本机 m2/工具盘/多镜像均无该 jar，网络不可达
   （repo1/aliyun 均 000）。⇒ Flink SQL 侧直连 Doris 的「查询期 JOIN」路线废弃，
   改 Python 侧导出复算（本就有 `_doris_scalar_runner` 同源执行面）。
2. **全量对账实测**（`src/ecomgen/reconcile.py`，已建成）：从 main topic 导出
   41495 条同批字节，用与 Java 闸门同源的 `validate_event` + 与
   `AggregateWindow` 同源的公式复算期望（25553 计数 / 10873 键），与 Doris
   `is_settled=1` 的 `event_*` 行（20335 / 11088 键）比 ⇒ **差 4673 行**。
3. **归因**（不是判据缺陷，是数据形态问题）：
   - main topic 是**历史多轮 live 的混合体**：25558 个 distinct event_id 中
     1503 个 id 有 2–52 次投递；其中 15337 次投递的 `event_time` **各不相同**
     （live 多轮同 seed 重产、每轮整体平移 ⇒ 同 id 不同时间），仅 600 次是
     真同字节重投（failover 重放）。
   - 实时作业**无 HA**、历史多轮重启从 earliest 重放 ⇒ 重放时同窗事件的
     「到达序」≠ 原始发生序 ⇒ 部分事件被 `sideOutputLateData` 判**超期旁路**
     （只进 `ads_late_expired_count`=7622，不进 `ads_window_metric`）。
     抽样实证：14:17:00 窗 Kafka 侧 4 条同 id 唯投递的 page_view/c_00006，
     Doris 只计 3——第 4 条今天 10:10 重放时被旁路（该窗超期表恰有 6 条记录）。
   - 旁路表只有 `(window_start, late_lag_ms)` 两维且 `late_lag_ms` 是
     「event_time − window_start」的确定性偏移（非到达时刻），Unique Key
     覆盖合并 ⇒ **无法**反推每个 `(window, metric, dim)` 键被旁路多少。
     逐窗守恒式核对失败（360 个差异窗中仅 5 窗与超期表吻合）；总量守恒也
     只有弱形式（差 5218 vs 超期 7622，差 2404 无去处）。分区序模拟旁路
     （18072）也不符——console-consumer 分区导出序 ≠ 历史真实消费交错序。
   - **结论**：全量逐值对账在历史数据上**原理性不可达**（信息已不可重建，
     非实现技巧问题）。⇒ 按裁决 C 的精神「不修历史、建对账专用切面」，
     改用**受控探针批**（§2.5）。

### 2.5 受控探针批对账（已建成代码面，实机验证进行到一半被暂停）

**形态**：`src/ecomgen/reconcile_probe.py` ——
1. 造 24 条探针事件：`event_time` = now−30 s 起 +300 ms/条（近窗 ⇒ watermark
   干净）、uuid event_id（与历史零重叠）、保留 category `c_recon`（键集隔离）、
   type 轮换 page_view/add_to_cart/view_item（add_to_cart 补 quantity）；
2. 投**同一条 Kafka 主 topic**（宿主 29092，fault.py 的 FEED_BOOTSTRAP 同源）；
3. 等 200 s（窗口关 60 s + allowedLateness 120 s + 缓冲，设计值）⇒ Doris 落定；
4. 期望复算走 `reconcile.expected_rows_from_events`（同一闸门同一公式）；
5. 对账**限定探针键集**（Doris 侧 `WHERE dim_key='c_recon' AND is_settled=1`）⇒
   逐值严格相等可判；历史键集不进判据（§2.4 归因登记）。

**代码面已落地**：
- `src/ecomgen/reconcile.py`（全量复算面，含 `expected_rows_from_events`
  /`parse_doris_rows`/`check_reconciliation_from_rows` 纯函数）
- `src/ecomgen/reconcile_probe.py`（探针面，`build_probe_events`/
  `check_probe_reconciliation` 纯函数 + `run_probe_reconciliation` 入口）
- `tests/test_reconcile.py`（13 用例）+ `tests/test_reconcile_probe.py`（5 用例）
  —— **全绿**（`uv run pytest tests/test_reconcile*.py -q`：18 passed）
- 未接线进 `verify_runner.py`（等实机验证通过后再接，避免半成品进五项面）

**实机状态**：`run_probe_reconciliation()`（投递→等 200 s→比对）刚启动即被
用户暂停——**尚未取得第一次实机结果**。恢复时从这里继续。

### 2.6 恢复施工顺序（下次会话从这里继续）

1. 实机跑 `run_probe_reconciliation()`（约 4 min：投 24 条 + 等 200 s + 比对）；
   预期 PASS（探针键集隔离 ⇒ 无历史污染）。若 FAIL：先查探针窗是否真落定
   （`is_settled=1` 出现否）、再看 watermark 是否推进（探针批之后若无新事件，
   窗口可能一直不关——需要跟一条「踢水印」的后续事件，设计点已在脑内）。
2. 反例（票面 AC 第 2 条）：改 Doris 侧一个探针落定值（UPDATE 一行
   `metric_value`）⇒ 对账 FAIL 退非 0 ⇒ 恢复（UPDATE 回原值或重投探针批）。
3. 负例（AC 第 3 条）：探针批期间造一个 `is_settled=0` 行差异（如只投不等的
   暂定行），证明不判——实际形态：探针对账 SQL 天然只取 `is_settled=1`，
   用一条「暂定期间的 Doris 快照」跑 `check_probe_reconciliation` 证明
   缺席行不导致失败形态（单测已含 `test_probe_recon_ignores…` 骨架，补实测）。
4. 探针对账接线进 `verify_runner.py`（第六项？还是附加断言？——按 spec §4.3
   P2 行「五项真断言」已满，探针对账以**附加断言**形态接入，不动五项面）。
5. dlq-rate 复核（387 基线两连续绿已过，观察是否再漂）+ impl-02e 八项复核。
6. 官方未给出登记补三条：③ JDBC catalog 工厂缺失（flink-jm 镜像无 connector-jdbc，
   官方 quickstart 未提 sql-client 需另装）；④ console-consumer 分区导出序
   ≠ 消费交错序（官方无此语义保证条文）；⑤ mariadb 驱动兼容 MySQL 协议但
   无 CatalogFactory 实现（实测 factory 清单只有 generic_in_memory/paimon*）。
7. 删临时文件（`_jobs.py`、`.uv-cache/fp03f_clear_dwd.sql`、
   `.uv-cache/probe_doris_catalog.sql`）；code-review → 全套 pytest → commit。

### 2.7 本会话环境面事实（备查）

- TM `slotsNumber=8` 已生效（§2.1）；流作业 `79fc4e42` 持续 RUNNING。
- flink-jm usrlib 里多了一个 `mariadb.jar`（591 KB，从 doris-fe 拷的，
  JDBC 探针尝试的残留）——**无用且未进 compose/git**，下次 deploy-job 会与
  作业 jar 同存 usrlib；无害但建议删（`docker exec eco-flink-jm rm
  /opt/flink/usrlib/mariadb.jar`）。
- Doris `updated_at` 大量行=2026-10-24 10:10:51（今天 deploy-job 重放刷写）——
  这不是数据事故，是无 HA 重放的固有行为，§2.4 归因链的一部分。
- 探针 category 用 `c_recon` 前缀——**不要**在其它场景复用该值（对账键集
  隔离的契约面）。

---

## §3 恢复施工收口（2026-10-24 20:47–22:00 会话，全部完成）

> **一句话状态**：§2.6 七步全部走完。探针对账经**三轮水位归因**后实机首绿；
> 反例/负例闭环；对账以附加断言接入 verify；基线重立后**两连续绿**
> （10 PASS / 退 0）；AC 全勾；impl-03 母票结清。

### 3.0 开工面环境事故（本会话第 3 只）

WSL 侧全部容器 20:47 被 SIGTERM 重启（`unless-stopped` 自启，RestartCount=0）
⇒ 无 HA session 作业全丢（§1.3 事故 1 同款第 3 次）⇒ `deploy-job.sh` 重提
流作业 `156f9846` RUNNING；顺手清掉 usrlib 残留 `mariadb.jar`（§2.7 备忘）。
执行面确认：宿主 `uv run` 因 `.venv/lib64` 是 WSL 符号链接而拒绝（跨系统
venv），一切 Python 实机操作走 `wsl -e bash -lc "uv run …"`。

### 3.1 探针对账实机首绿（§2.6-1）——三轮水位归因

**跑 1（复演暂停前状态）FAIL**：差异 9 行全是 `期望=None`（12:18/12:19 旧
探针批残留行）。两个问题被本轮同时暴露：
- 踢水位事件投在批**前**（复演时序错误）⇒ 本批窗口没有关闭计时；
- 对账 SQL 无窗口范围过滤 ⇒ 上一轮探针批（重放后落定）污染本批判定。

**修法 1**：批后投 tick + `PROBE_DORIS_SQL_TEMPLATE` 加
`window_start BETWEEN 本批[ws_min,ws_max]`（`probe_window_bounds_ms` 从批
自身事件推范围，每轮必新）。

**跑 2 FAIL**：本批 13:11 窗整窗未落定（期望 8/8/8 实测全空），但旧批残留
已不再污染（范围过滤生效）。查 Doris：13:11 窗只有 `provisional_emitted=24,
is_settled=0` ⇒ 事件进了聚合状态、窗口没关。

**水位数学定案**：watermark = max(event_time) − 120 s
（`GateSegment.mainSource` 的 `forBoundedOutOfOrderness(ALLOWED_LATENESS)`）；
窗口 [ws,ws+60s) 关闭条件 watermark ≥ ws+60s ⇒ 需要事件 et ≥ ws+180s。
批时即时 tick 的 et≈投递时刻，只能推 watermark 到「此刻−120s」——**永远
追不上当前分钟窗的关闭线**。修法 2：等 200 s（钟走过 ws+180s）后投**第二条**
tick ⇒ 此时 et=now ≥ ws+180s ⇒ watermark ≥ ws+60s ⇒ 窗关。

**跑 3 FAIL（最后的真因）**：13:19 窗在第二条 tick 后**仍**未落定。分区诊断
（`kafka-get-offsets` + 逐分区尾消息）实锤：6 分区里批事件散落 p0/p2/p3/p4，
两条 tick 只落 p1/p5 ⇒ **operator watermark = 各 source subtask 水位的 MIN**，
被钉在 p3 的 `批内最小 et −120s`。13:11 窗恰好是被跑 3 批事件（et 13:19）
推过去的——单 tick 从未生效过。修法 3：`send_watermark_ticks()`
**逐分区**各投一条 et=now（confluent_kafka `partition=` 显式指定）。

**跑 4 PASS**：`rows=3 探针键集逐值全等`（page_view=8 / add_to_cart=8 /
view_item=8，24 事件 = 8×3 轮换，@13:32:00 UTC 窗）。

### 3.2 反例（AC 第 2 条）：改一个落定值 ⇒ FAIL 退非 0

`UPDATE ecom_ads.ads_window_metric SET metric_value=metric_value+1000`
（13:32 窗 event_add_to_cart 行，is_settled=1）⇒ 对账
`FAIL 差异 1 行: event_add_to_cart@2026-10-24 13:32:00: 期望=8 实测=1008`；
UPDATE 回原值 ⇒ 复绿 `rows=3 全等`。闭环成立。

### 3.3 负例（AC 第 3 条）：is_settled=0 不判

实测暂定行共 5 窗、全部 `provisional_emitted`（12:19/13:02/13:11/13:19/13:32，
值 6–24 不等，与落定口径天然不同）。**双保险**（code-review 后补齐判定面）：
① 采集 SQL 过滤 `is_settled=1`；② 判定面 `check_probe_reconciliation` 词表
只认 `event_*`（provisional_emitted/uv 不进比对）——单测
`test_probe_recon_ignores_unsettled_provisional_rows` 把「混进暂定行的采集
结果」直接喂判定面证明 PASS 不受影响（AC 第 3 条的判定面负例）。
~~实测暂定行与落定口径不同 ⇒ 采集过滤天然不可见~~（原表述把 SQL 单保险
说成双保险，已由判定面过滤补真。）

### 3.4 接线 + 两连续绿（§2.6-4/5）

- `check_recon_probe` 接入 `P1_EXTRA_CHECKS` 第 4 位（漏斗/脏率/两态/**对账**），
  五项面不动（spec §4.3 P2 行「五项真断言」由本体满足，对账是行为判据）。
- 首遍 verify：9 PASS / 1 FAIL——唯 `dlq-rate`（432/41631 vs 基线 387/41495，
  +45）。归因：DLQ 432 条里 `schema_invalid×00000000-… 段`=210（基线期 165），
  增量 +45 全是该 fixture 段在**本会话 JM/TM 三次重启** earliest 重放下的又一
  轮重投；recon/wm-tick 相关 = **0 条**（探针零污染）。与 §2.2 同源：环境
  冲击垫高，非系统退化 ⇒ 基线重立 432/41631（吸收式记账同口径）。
- **两连续绿**：`verify-report-20261024T135604Z.json` 与 `…T140141Z.json`
  均 `PASS=10 FAIL=0 SKIP=0 ⇒ 退出码 0`（五项真断言 + 漏斗/脏率/两态/对账/
  降级共 10 项，第三项 `paimon-five-layers` 以真断言形态 PASS）。
  - **时间线算术质疑的终裁**（code-review 提出「337 s 间隔装不下两轮全量」）：
    单轮 verify 实测 **330 s**（计时重跑 `T143624Z`，rc=0）：五层探针在
    03d 维护段 compaction 后的表上是**秒级**（`~2 min` 是冷读观察的保守
    上界），260 s 对账硬等待 + 五项 + 其余判据 ≈ 330 s ⇒ 337 s 间隔成立。
  - code-review 修复后的**复绿**：`T143624Z`（check 改名
    `recon-offline-mirror-doris` 后）仍 10 PASS / 退 0。

### 3.5 impl-02e 八项 P1 判据复核（AC 第 6 条）

| impl-02e 判据 | 本会话验证面 | 结果 |
| --- | --- | --- |
| init 后 verify 退 0、ps 全 healthy | 两连续绿退 0；verify 前提即集群 healthy | ✅ |
| Doris 断言经 client 且查「有行」 | `doris-ads-rows` 走 `_doris_scalar_runner`（compose run client）逐表 PASS | ✅ |
| 迟到修正/超期计数 | `is-settled-two-states pending=677 settled=11904` PASS；ads_late_expired_count=7727 有行 | ✅ |
| 五项打印 + 窗口关闭可观察 | 五项逐条打印（第三项 P2 转真是既定演进）；两态判据 PASS | ✅ |
| verify-fault 不重跑（impl-02d 已证） | 引用既有证据，本票未动 fault.py | ✅ |
| 改坏输入 ⇒ 退非 0 | 本票 §2.3 清层反例 + §3.2 篡改反例（同性质） | ✅ |
| JSON 逐断言 + FAIL 快照 | 两份 JSON 报告落 `artifacts/verify/` | ✅ |
| 超时数字标设计值 | verify_runner 新增常量（PROBE_* 等待、tick 语义注释）均标设计值 | ✅ |

### 3.6 impl-03 母票 10 条验收逐条核对（见母票勾选）

1/2/3（03b/03c 形状与分层跑通）· 4 幂等（03d）· 5 反例证伪（03e）·
6 快照区间（03d）· **7 对账逐值相等+篡改反例（本票 §3.1/§3.2）** ·
**8 verify 第三项转真且五项全真退 0（本票 §3.4）** · 9 data/ gitignore
（本票复核 `git check-ignore`）· 10 §8.3 登记（本票五条 + 各副票）。

### 3.7 收口动作

- impl-dag：impl-03 结清，Frontier = impl-04（impl-05 双闸门只剩看板一扇）。
- 临时文件清理（`_jobs.py`、`.uv-cache/fp03f_clear_dwd.sql`、
  `.uv-cache/probe_doris_catalog.sql`、死文件 `sql/verify_paimon_recon.sql`）、
  code-review、全套 pytest、commit。

### 3.8 code-review 处置记录（两轴并行子代理，2026-10-24 22:15）

**Standards 轴（2 硬违规 + 6 判断项）**：

| # | 发现 | 处置 |
| --- | --- | --- |
| S1 | 内联超时未标设计值（reconcile 90/30 s、probe 120/10/30 s） | ✅ 已修：`KAFKA_METADATA_TIMEOUT_S`/`KAFKA_FLUSH_TIMEOUT_S` 常量 + `DORIS_QUERY_TIMEOUT_S` 复用，全部标注 |
| S2 | 恒真断言 `assert … or True` | ✅ 已修：改为对第三项 status/name 的真断言 |
| S3 | mysql 命令块三处复制 | ✅ 已修：抽 `reconcile_probe.doris_batch_query`（与 `_doris_scalar_runner` 同形，跨模块抽公共层留待后续——两处签名语义不同：标量 vs 批量行） |
| S4 | 死代码：全量对账采集器零调用 | ✅ 已删：`run_reconciliation`/`export_kafka_lines`/`collect_doris_rows` 移除，头注保留归因指针（§2.4 + spec §8.3），防后人重走 |
| S5a | `P1_CHECKS` 名字承载 P2 语义 | 记录不改：历史判据名（impl-02e 验收面引用它），改名是跨票重构 |
| S5b/c | 两个名不副实测试 | ✅ 已重写：`fails_when_foreign_category_leaks` / `fails_when_stale_probe_window_leaks`（名实相符且真注入） |
| S6 | 测试镜像计数循环复制 5 次 | ✅ 已修：抽 `_mirror_counts`/`_as_doris_keys` helper |
| S7 | `check_recon_probe` 纯转发 | 记录不改：P1_EXTRA_CHECKS 接线惯例 |
| S8 | 跨模块导入私有 `_epoch_ms_to_utc_string` | ✅ 已修：转正公共 `epoch_ms_to_utc_string`，删兼容别名 |

**Spec 轴（4 实质发现）**：

| # | 发现 | 处置 |
| --- | --- | --- |
| P1 | 「对账未读 Paimon」：探针面是**离线公式镜像 vs Doris**，非 Paimon 表直比；裁决 C 的执行形态偏离原文且未再裁 | ✅ **承认并诚实化**：check 改名 `recon-offline-mirror-doris`、模块头注/票面 §3.1 明示「不虚称读到 Paimon」；形态依据＝§2.4 全量对账原理性不可达的实测归因 + 裁决 C 的精神（「不修历史、离线侧同源复算」）。**是否需用户再裁**：留待 impl-05 终验或用户过目本节时定——对账的「离线口径基准」语义由镜像公式（与 AggregateWindow 逐字同源 + validate_event 同闸门）承载，这正是 03c 移交时「同名指标同一公式」的可判形态 |
| P2 | TM slot 4→8 未记用户裁决 | §2.1 已核冻结面（REQ-F-RT-08 限并行度非 slot 数）并留动机注释；**裁决缺位属实**——slot 是逻辑数不改资源面，风险极低，impl-05 终验清单里向用户报告追认 |
| P3 | 每次 verify 向看板供数表注入 24+12 行 | 承认（取舍已定）：键集隔离（c_recon / wm-tick 的 c_00000 分钟窗）对看板聚合无干扰；替代方案（独立 topic/影子表）会偏离「与实时作业同一入口」的对账本意。**impl-04 看板票开票时须把「探针行排除出看板 SQL」列入其验收** |
| P4 | dlq 基线重立=洗白风险（+45 超 ATTRIBUTABLE_REPLAY_FACTOR 上限） | ✅ **已收紧**：ATTRIBUTABLE_REPLAY_FACTOR=3 的上界只约束 `--corrupt` 注入路径的吸收记账（fault.py），不约束环境重启重放——但「手工改基线文件」确无防护。补一条规则进本票 Notes：基线重立必须附「逐条归因证据链」（§2.2 的 DLQ 全量分解表 / §3.4 的 432 分解），无归因不得重立；该规则的代码化（如基线文件带 attribution 字段）记 impl-05 待办 |

**测试面修复后的质量门**：`pytest` 受影响面 57 passed（reconcile/verify 四文件）；
全套 304+ passed；ruff check/format 干净；basedpyright errors=0。
复绿证据：`verify-report-20261024T143624Z.json`（10 PASS / 退 0，
`recon-offline-mirror-doris` 新名生效）。