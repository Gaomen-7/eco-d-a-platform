# impl-06i: 部署面判据 ⑤ 排除集不完备 — 工作树噪音与非 SQL 文件把「零 ADD JAR」打成恒假

Type: task
Status: ready-for-human

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

- [ ] 裁决修法方向（A/B/C 或组合）并记录理由
- [ ] 实施后不设 override 复跑 `deploy_surface_check.sh`：⑤ PASS 且其余 10 条不回归
- [ ] 反例凭证：向 sql/ 注入一条真 `ADD JAR '<宿主路径>'` 语句 ⇒ ⑤ 必 FAIL；还原后 PASS
- [ ] `.mimosa/` 的 gitignore 归置（忽略或保留跟踪）随本票一并裁决——工作树噪音
      是否该进 `.gitignore` 是它自己的问题，但它让 ⑤ 恒假是事实

## Notes

- 实证凭证：`.scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/01-deploy-surface-check.out.txt`
  ⑤ 段（32 行命中逐行列出）。
- 关联：impl-03a §D（判据缺陷谱系）、impl-06e（判据口径变更先例）、
  impl-06f-4（发现本轮实测的收尾门）。
- 判据 ⑤ 的**原始语义**（官方依据：ADD JAR 指向宿主路径在分布式集群不可行，必须
  构建期 COPY）不受影响——本票只修「扫哪些文件」，不修「判什么」。
