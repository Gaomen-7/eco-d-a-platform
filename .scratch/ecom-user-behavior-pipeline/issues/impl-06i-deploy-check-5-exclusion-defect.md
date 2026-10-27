# impl-06i: 部署面判据 ⑤ 排除集不完备 — 工作树噪音与非 SQL 文件把「零 ADD JAR」打成恒假

Type: task
Status: resolved

Blocked by: —

**What:** `deploy_surface_check.sh` 判据 ⑤「全仓无 ADD JAR 指向宿主路径」在 2026-10-26
收尾门实跑（impl-06f-4，无 override、退出码 1、10 PASS / 1 FAIL）中命中 **32 行**，
逐行归因后**全部不是可执行 SQL**：

| 命中来源 | 行数 | 形态 | 为什么是噪音 |
| --- | --- | --- | --- |
| `.mimosa/hook-state/**/*.source` | 27 | DSH harness 会话状态目录里**本仓文件的 baseline 副本**（含 `deploy_surface_check.sh` 自身与 `tests/test_replay_sql.py` 的快照） | 不是仓库源码：未跟踪（`git ls-files` 零）、未 gitignore、判据写就（2026-10-22）时**该目录不存在**（2026-10-25 才出现） |
| `tests/test_replay_sql.py:17/160/163/170` | 4 | 守卫测试的 **docstring 与断言消息**（「`sql/` 里一条 `ADD JAR` 都没有」） | 讽刺闭环：它守卫的就是这条判据的目标，自己却被判据命中。Python 注释/docstring 不是 SQL |
| `README.md:64` | 1 | 「部署面判据」小节**列举判据名**（impl-06f-1 新增） | Markdown 叙述不是 SQL |

**判据的既有排除法为什么抓不到它们**：脚本只排除 `#` 开头的注释行
（`grep -v -E '^[^:]+:[0-9]+:[[:space:]]*#'`）与脚本自身路径——覆盖 SQL/脚本注释，
覆盖不了 **Markdown 叙述、Python docstring、harness 状态目录**三种形态。

**归类**：判据自身缺陷（恒假方向），非部署面真缺陷、非宿主环境。
与 impl-03a §D #2 的「计数重复」、impl-02c 的两条判据缺陷同一谱系：
判据面的缺陷单独开票，不在验证票里顺手改（impl-06f-4 Notes 明令）。

## 修法方向（三选一或组合，需裁决）

- **A：排除集加 `.mimosa`**（`--exclude-dir=.mimosa`）——最小改动，治「harness 状态目录」
  一类；但下次出现别的未跟踪目录（工具缓存、IDE 状态）仍会恒假。
- **B：只扫 git 跟踪文件**（`git grep -iI 'ADD JAR'` 替代 `grep -r .`）——治本：
  判据对象本来就是「**仓库源码**」，不是「工作树里的一切文件」。副作用：
  依赖 git（干净检出场景成立；`git grep` 的排除语义与 grep 不同，需同步复核
  ⑤ 的附加扫描段「宿主路径 jar 引用」是否也要换面）。
- **C：按文件类型过滤**（只扫 `*.sql` / `*.java` / `*.sh` 等可执行面）——判据对象收窄为
  「可能出现 `ADD JAR` 语句的文件」；副作用是清单要维护，新增源码类型时要记得加。

**注意**：任一方向都改变 ⑤ 的口径 ⇒ 按仓库先例（impl-06e），改完须出
「改前必红 / 改后必绿」的反例凭证（例如：注入一行真 `ADD JAR` 进 sql/ 必须仍被
抓到——防止收窄排除集把真缺陷一起放走）。

## Acceptance criteria

- [x] 裁决修法方向（A/B/C 或组合）并记录理由 —— **2026-10-27 用户裁决：C（可执行面
      白名单）单用**，理由见 Answer §1
- [x] 实施后不设 override 复跑 `deploy_surface_check.sh`：⑤ PASS 且其余 10 条不回归
      —— `04-clean-full-gate.out.txt`：11/11 PASS、退 0、COMPOSE_EXTRA_FILES 为空
- [x] 反例凭证：向 sql/ 注入一条真 `ADD JAR '<宿主路径>'` 语句 ⇒ ⑤ 必 FAIL；还原后 PASS
      —— `02-red-sql-addjar.out.txt`（10 PASS / 1 FAIL，唯一 FAIL＝⑤，恰好命中注入行）；
      另补 seg2 腿反例 `03-red-sh-hostpath.out.txt`（.sh 宿主路径 jar 引用、无 ADD JAR
      字样 ⇒ seg1 零命中、seg2 抓到，同样 10 PASS / 1 FAIL）
- [x] `.mimosa/` 的 gitignore 归置（忽略或保留跟踪）随本票一并裁决 —— **2026-10-27
      用户裁决：忽略**。`.gitignore` §7 已落 `.mimosa/`（`.gitignore:97`），
      `git check-ignore -v .mimosa` 命中、`git status` 不再列出它

## Notes

- 实证凭证：`.scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/01-deploy-surface-check.out.txt`
  ⑤ 段（32 行命中逐行列出）。
- 关联：impl-03a §D（判据缺陷谱系）、impl-06e（判据口径变更先例）、
  impl-06f-4（发现本轮实测的收尾门）。
- 判据 ⑤ 的**原始语义**（官方依据：ADD JAR 指向宿主路径在分布式集群不可行，必须
  构建期 COPY）不受影响——本票只修「扫哪些文件」，不修「判什么」。

## Answer（2026-10-27）

**结论：按用户裁决的方向 C（可执行面白名单）实施完毕；反例两腿必红、还原后全门
11/11 绿；`.mimosa/` 随票进 `.gitignore`。**

### §1 裁决记录（用户，2026-10-27）

裁决前两轮模拟实测（当前工作树）推翻了票面隐含假设——A、B **单独都到不了 PASS**：

| 方向 | seg1 结果 | seg2 结果 | 判定 |
| --- | --- | --- | --- |
| A（排除 `.mimosa`） | 仍命中 5 行：README:73 + `tests/test_replay_sql.py` ×4 —— 它们是**被跟踪**的叙述行 | 18→0 | ✗ 恒假未除 |
| B（只扫 git 跟踪文件） | 同上 5 行仍在（git grep 排不掉被跟踪叙述） | 0 | ✗ |
| **C（类型白名单）** | **0**（`*.sql/*.sh/*.java`+`Makefile`/`Dockerfile`/`*.yml/*.yaml` + 注释过滤后零命中） | **0**（`.mimosa` 4050 个文件全是 `.source`/`.json`，天然不在白名单） | ✓ |

用户据此裁决 **C 单用**（不叠 B）：白名单同时对「未来别的未跟踪垃圾目录」结构性免疫，
且不引入 git 依赖、还能抓到未 `git add` 的新 `.sql`（B+C 组合反而在 commit 前有盲区）。
第二问裁决：**`.mimosa/` 入 `.gitignore`**（与 `.dsh/`、`.agents/` 同族——DSH harness
会话状态不入库；在 C 之下它与判据 ⑤ 已解耦，纯属仓库卫生）。

### §2 新发现（票面未记）：seg2 同样恒假

复盘时实测：⑤ 的**附加扫描段**（宿主路径 jar 引用）在 `.mimosa/` 存在时同样恒假——
18 行命中全是 harness 基线里的 `docker cp …:/opt/flink/lib/ecom-flink.jar` 快照。
06f-4 实跑中 seg1 先 FAIL 提前 `return`，把 seg2 的恒假挡住了。故本票**两段同改**，
并对 seg2 也出了反例凭证（见 §3 第三轮）。

### §3 实施与证据

改动只有两处（零运行面变更）：

- `scripts/deploy_surface_check.sh` ⑤：两段 grep 收敛为共用 `SCAN_INCLUDE`（白名单）
  + `SCAN_EXCLUDE`（原有排除目录）两组数组；注释行过滤从仅 `#` 扩到 `#`/`--`/`//`
  三种注释形态（覆盖白名单内的 sh/yaml/Make/sql/java）。判据语义（「判什么」）不变。
- `.gitignore` §7：加 `.mimosa/`（附裁决由来注释）。

证据（`.scratch/ecom-user-behavior-pipeline/evidence/impl-06i/`）：

| 文件 | 内容 | 结果 |
| --- | --- | --- |
| `01-function-smoke.out.txt` | 函数级冒烟：干净树 PASS ⇒ sql/ 注入真 ADD JAR FAIL ⇒ scripts/*.sh 注入宿主路径 jar FAIL ⇒ 还原 PASS ⇒ 探针零残留 | 全过 |
| `02-red-sql-addjar.out.txt` | **全门**反例 A：sql/ 注入 `ADD JAR '/mnt/f/…jar'` | 10 PASS / **1 FAIL（仅⑤，命中注入行）**，退 1 |
| `03-red-sh-hostpath.out.txt` | **全门**反例 B（seg2 腿）：scripts/ 注入 `HOST_JAR=/mnt/f/…jar`（无 ADD JAR 字样） | seg1 零命中、seg2 抓到；10 PASS / **1 FAIL（仅⑤）**，退 1 |
| `04-clean-full-gate.out.txt` | 还原后**不设 override** 全门复跑 | **11/11 PASS，退 0**，⑤「ADD JAR 零命中 + 宿主路径 jar 引用零命中」 |
| `05-restore-job.out.txt` | 环境归位：usrlib 重拷 + 作业恢复 RUNNING + lag=0 | RUNNING（5s）、LAG=0 |

静态面：`pytest` 340 passed、`make lint` PASS、`make typecheck` PASS（errors=0）。

### §4 过程事故（环境，非判据回归）

02 首轮撞上 Docker Desktop/WSL2 宿主事故：`eco-flink-tm` 容器 PID 498 变 zombie
（`cannot stop container … zombie and can not be killed`），③ 的 down 半途失败，
栈半拆除。重试 `down` 清场 → `up -d` 恢复健康后**重跑 02** 拿到干净证据
（首轮污染输出已被覆盖，不在证据链里）。与判据改动无关；记录在案只为解释
02 的 `START_UTC` 与栈重建时间线。

### §5 遗留说明

- 白名单是**维护点**：未来新增可执行源码类型（例如某种新的 SQL 载体文件）时，要把
  扩展名加进 `SCAN_INCLUDE`，否则该类型脱管。已写进脚本头注（⑤ 段）与本行。
- `.py` 不在白名单是故意的：本仓 Flink SQL 一律走 `sql/*.sql` 文件经 `sql-client -f`
  执行（`lake_replay.sh` / `lake_assert.sh` / `verify_runner.py` 均如此），Python 不内嵌
  SQL；守卫测试 `tests/test_replay_sql.py` 的 docstring 属叙述面，按裁决免疫。
- 判据 ⑤ 原始语义（ADD JAR 指向宿主路径在分布式集群不可行、必须构建期 COPY）不受影响
  ——本票只修「扫哪些文件」。
