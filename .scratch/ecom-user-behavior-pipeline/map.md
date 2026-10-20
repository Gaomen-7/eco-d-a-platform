# MAP — spec-ready 路线图（电商用户行为数据全链路）

Kind: map
Label: wayfinder:map
Status: open
Created: 2026-10-20
Progress: 9 票，**6 已落子**（P1 最小可证集 · 唯一真相源 · Paimon/Flink/Doris 一手事实 · 可复现边界 · Doris 容量与可证性 · **离线分层范围与五层形状**）｜**现在的前沿：[03]（[05] 已落子，唯一阻塞已解除）**｜迷雾 **4 条**（F2/F4 已收口）｜图形见 [map-dag.md](issues/map-dag.md)

> **本文件是这张地图的唯一权威**；依赖图见 [map-dag.md](issues/map-dag.md)（辅助视图，与本文逐条对齐）。
> **上游档案**：`SRS.md`（必须做到什么）→ `gdd.md`（每个功能怎么做）→ `HLD.md`（结构、栈定案、运行约束）→ `FSR.md`（路线冻结）→ `hld-research-*.md`×8（一手证据）。
> **回溯**：`HLD.md` §0.1 自称「定位这一层的唯一入口」——**那是上一代设计的说法**。自 2026-10-20 起，本目录的入口由 **AGENTS.md 的 Design docs 指向 `map.md`**；两者冲突时以本节 + AGENTS.md 为准。
> **粒度**：2026-10-20 由 11 票精简为 9 票——研究票按「一手事实」归并、判定标准的票并入语义票、schema 形状并入它们真正的所有者。

## Destination

写出一份 **spec-ready 的路线图**：解到 frontier 为空时**唯一**还站着的规划产物，承载 P0 环境 / P1 实时闭环 / P2 离线分层 / P3 文档四段的**定案**，供下一个 agent 会话直接施工，不再回溯 HLD/GDD/SRS 的推理现场。

**边界**：只到「知道怎么施工、判成没成」为止。实现本身、README 与 GIF 的制作、亿级回放的实际执行都在目的地之外。
**不在目的地里**：组件版本锚点（C3 冻结）、路线选型、冻结端口之外的新端口、Makefile 八个冻结目标名、`ecom_ads` / `ecom_lake` 库名——要改走冻结项变更，不在这张图上解。

## Notes

- **域**：单机可复现的电商用户行为数据全链路（Kafka → Flink → Paimon / Doris → Superset）。`GLOSSARY.md` 是唯一术语表。
- **会话开始前必读**（按序）：`AGENTS.md` → 本文件 → `issues/map-dag.md` → 你选中的那张票。
- **技能**：`grilling` + `domain-modeling`（HITL）；`research`（AFK，可并行）；`prototype`（HITL）。
- **常驻纪律**（逐条有出处）：
  1. **参数性质边界**：数字标 `官方给出` / `设计值`，**不得把设计值当官方保证**（HLD §0.3）。
  2. **行为可证**：每条约束附检查方式与判定条件；「配置可见」「UI 里能看到」不是判据（HLD §0.4、gdd §0）。
  3. **实机验证**：格式/镜像内容/数据落点/指标可信度**先验证再写文档**——P0 已在四处被官方说法打脸（HLD §8-R7）。
  4. **能起来 ≠ 能重起来**：状态进卷的组件，验收必须含一次「不删卷的重启周期」（HLD §7.3）。
  5. **代理测试必须用真实同类对象复验**（HLD §8-R4）。
  6. **git**：agent 只本地提交，**不推送**。
- **本地改动先获批**：先征得同意、先备份、说明回滚，再执行。
- **票的物理形态**：`issues/NN-<slug>.md`；`Status:` ∈ `open` / `claimed` / `resolved`；`Blocked by: NN, NN`；`Type:` ∈ `research` / `prototype` / `grilling` / `task`。
- **一次会话只解一张票**（research 票例外）；解前先把 `Status: claimed` 写盘。
- **只以 `issues/*.md` 为准**：票号即文件名前缀，任何 `Blocked by:` 里的 `02` 若出现在票 02 自身，指的是**地图票 D4**（已落子），不是自环。

## Decisions so far

- [**P1 实时闭环的最小可证集**](issues/02-p1-realtime-closure-contract.md#answer)：本票从「P1 做什么」收窄为「P1 怎样算成立」，圈表（PI-1/2/3）留给你裁。连带落定：目的地收窄为 spec-ready 路线图；冻结面不进图；设计面就地落子。
- 正文级已决（不出票，只作上下文）：
  - **(D1) 看板终点＝Superset**：七项候选各有一条判据天然做不到；排除理由**不是许可证**（HLD §3.5）。
  - **(D2) 平台形态**：`client` 承担 Stream Load 两跳（外移即失败，§4.4/§8-R2）；类型门禁＝`basedpyright` pin `1.40.2`（§3.4）；工具链 uv / Python 3.12 / Ruff / JDK17+Maven（§3.2）；会话沙箱不需要 `danger-full-access`（§4.6）。
  - **(D3) 执行面**：单一执行面＝WSL2（含一次「用代理对象做代理测试」的推翻，§4.7/§8-R4）；超集 schema 由生成器与公开数据集共用（FSR §5）。
  - **(D4) 配置确定性与可复现＝立项基石**：目的地收窄、冻结面不动、设计面就地落子；**执行面**（三份 P0 交接的合流、HLD 的重复段与体积口径）明确交给 [04] 去做，不在这里空转。
  - **(D5) 唯一真相源＝`canonical-truth.md`**（票 [01] 已落子，裁决全文见 [canonical-truth.md](canonical-truth.md)）：①**权威模型改双轨**——分档界线＝**事实 vs 意图**，事实类可直接改写上游、意图类只登记并需签字；②**处置词表封闭**（`改写上游`/`覆盖下游`/`改写上游(需签字)`/`降级为可选`/`补充落点`/`挂起待证`）；③**收口产物不做 `spec.md`**（与 SRS.md:6 冲突，且「收口」≠ [09] 的施工规格）——改为 `canonical-truth.md` + SRS 修订清单 + 偏离表规范化；④**本票只产口径，改字由 [09] 统一落笔**（例外：[08] 就地改它自己那条）；⑤三条规则铁律定案：**`schema_version`**（minor/major 分级、消费端容忍同 major 更高 minor、拒更高 major 原因码 `schema_major_unsupported`、版本号不要求相等、checkpoint 恢复按「缺省值+标记」放行）、**离线 DDL 三原则**（时间维分区键必含、拒绝表必须 append、拉链时间源同事件时间）、**迟到计数表三原则**（落 Paimon `ecom_lake`、append、`dt`×迟到分桶只存计数）。**12 条冲突的签字与冻结项变更已全部闭合**（2026-10-20 用户签 D/H/I、批 FC-1/FC-2）⇒ 状态为待执行 **9**（A/B/D/E/G/H/I/J/L）、挂起待证 **1**（C）、已收口 **2**（F/K）；**落笔统一由 [09] 执行**（例外：[08] 就地改它自己那条），输入已齐备。
  - **(D7) 可复现的主体＝制品，不是平台**（票 [04] 已落子，产物 [`reproducibility-boundary.md`](reproducibility-boundary.md)）：①**四档契约 L0–L3**（制品态 / 零状态冷启 / 带卷重启 / 跨机搬运），`REQ-F-ORCH-01` 的「一条命令」**只认 L0**，L1 **豁免** 30 分钟（Doris 必须本地构建 ≈35 min）；②**「一条命令」具名为 `make init`**（四段冻结，不新增冻结目标）；③**`client`**：`depends_on` 只等 `kafka`（HLD 旧文「全链路」会**成环**）、按需 `run --rm`、**原生 Linux 上可选**（官方「Linux hosts can reach the nodes directly」）；④**平台专属事实一律不进入承诺**（工具链无 sudo 解包、`.wslconfig`、WSL integration、镜像站可达性）；⑤**JDK/Maven 只服务 Flink 作业构建**，不在起链路路径上（迷雾 **F8 就此收敛**）；⑥`CAP-03/04` 三处**意图类改写已签字**，逐字新句与 `HLD.md:294` 的 F-1 订正**留给 [09] 落笔**。
  - **(D9) 离线分层的范围与五层形状**（票 [05] 已落子，全文见 [issues/05](issues/05-offline-layer-scope.md#answer)，2026-10-20，18 问全按推荐落子）：**上半**——①**强序＋判据分层**：P2 只在 P1 实时四项通过后开；`REQ-F-VERIFY-01` 第三项「Paimon 五层有行」在 P1 阶段是**显式 SKIP 不是 PASS**（签字版句子已给出，落笔归 [09]）——堵死今天 `make verify` 第三项打 `PENDING` 却不置 `fail=1` 的假通过；②**门槛批**＝生成器 `dump --events 1048576 --logical-span 3d --seed` 现产、落 `data/` 不入 git（C5），批次密度 ≠ 实时速率是两个参数；③**触发面**＝单目标 `replay` + `LAYER=`，零冻结项变更；④**槽位**＝复用 4 槽不加 profile（+3 GiB 方案算不平：13.5 > 11.68 天花板）。**下半（五层形状表）**——统一 `dt STRING` 分区；ODS append + `dt=落地日`（Q25 零解析优先于分区演示）+ `bucket=-1`（unaware，无索引代价）；DWD 主键 `(dt, event_id)` + 固定 `bucket=1` + `upsert-materialize=NONE`（不设 `sequence.field`，靠重放确定性）；拒绝表 append + `dt=摄入日`（处理时间——坏数据的 `event_time` 不可信）+ `reject_reason` 三值无 duplicate；DIM 拉链改 **`valid_from_ms`/`valid_to_ms`**（BIGINT ms，`end_date` 日期精度表达不了同日两次变更），join 走 **batch 区间 join**（lookup join 被官方逐字否定：processing-time，回放必错；Paimon 全站无 zipper/SCD）；DWS/ADS 主键含窗口起点且 **ADS 与 Doris `ecom_ads` 键同构**——**两套物化、一个口径**，对账归 [03]；迟到计数表落定 **`dwd_late_event_count`**、`dt`×60 s 档（F4 收口）；快照 **min=5 / max=20 / time-retained 默认**，判据 `$snapshots` count ∈ [5,20]；不变量「ODS = DWD + 拒绝 + 迟到标记差异」**删除**，改三条可判断言 A1（分流完备）/A2（重复只记账不断言）/A3（迟到对账）。**执行面**——P2 **全 SQL 零 jar**（F8 结论加强：JDK/Maven 连 P2 也不需要）；`sql-client -f` 退出码**不可用**（源码级：`CliClient` 丢弃失败返回值、无 `System.exit`）⇒ 断言＝`scripts/lake_assert.sh` 解析 stdout 哨兵；批读会丢 DELETE ⇒ 拉链断言分当前快照与 `$audit_log` 两条。**部署硬约束（新增）**——warehouse 落具名卷同路径挂 JM+TM；Paimon jar 构建期 COPY 进**每个** Flink 进程的 `/opt/flink/lib`。**对 [08] 两处订正**（R1：append 的 `-1` 是 bucket-unaware，「1亿key≈1GB 索引」不适用 ODS；R2：`retain_max` 是上界不是「钉住」）＋**10 条官方未给出**登记。**需批准 5 项已批**（SKIP 签字句、离线幂等判据、拉链改名、迟到计数表名、warehouse 卷）。⇒ [03] 解除阻塞；F2/F4 迷雾收口。
  - **(D8) Doris 容量与保留可证性的一手事实**（票 [07] 已落子，全文见 [issues/07](issues/07-doris-capacity-in-practice.md#answer) 与 [research/07](research/07-doris-dynamic-partition-retention.md)）：①**取证规则改立**——本仓跑 `4.1.5-release` 二进制 ⇒ 默认值/阈值**以该 tag 源码为权威**；本轮实测 **6 处** 4.x 文档页与出厂源码不一致（`dynamic_partition_enable` true/false、`max_dynamic_partition_num` 500/20000、BE `trash_file_expire_time_sec` 0/86400 等，其中第一项**官方两文档页先自相矛盾**）——HLD §8-R7 教训前移到文档站自身；②**gdd D8 被推翻一半**：`dynamic_partition_enable` 4.1.5 默认 **`true`** 不是 `false` ⇒「FE 总开关」这个第三前置条件**作废**（真正前置＝表级四项）；`init.sql:16` 幂等冗余且官方明写运行时设置在 FE 重启后失效；`start` 默认 `-2147483648` 的本仓主张**证真**；③**可证性定案**：`REQ-F-OLAP-03` 判据＝**「配置 + 小时级演练」**（`start=-30` 保留恰 30 个日分区、`create_history_partition=true` 建表当场 34 个分区可查＝配置层可证；`time_unit=HOUR`+小 `start` 的独立演练表 ≤1200 s 可观测真实删除＝行为层可证；**不声称**验证过 30 天边界）——HOUR 是文档+源码双重支持的正规粒度，但 DATE/DATEV2 分区列官方 DdlException 拒绝 HOUR ⇒ `ads_funnel_step` 无小时演练路径；调度间隔 600 s 官方给出；④**DROP 非立即回盘**：走 catalog recycle bin、BE 经 GC 异步回收，「磁盘 T 内释放」官方未给出；⑤**Kafka 同类但差一个量级**（体积上限 vs 时间上限），且 gdd D3 两条被修正：所引逐字句在 Kafka 4.3 未命中（官方逐字在 Log 页），`retention.bytes` **按分区**生效 ⇒ 6 分区下「≈6 h」可能实为 ≈36 h，**算术须按配置层级重算**（交 [09]）；⑥**compaction 成本**：Doris/Paimon 的耗时与放大数字均**官方未给出**，唯一官方系数＝env-checking 页「BE＝总量×3×**1.4**」（×1.4 逐字＝"40% reserved space for background compaction"，integrated+3 副本口径，本仓 1 副本仅 ×1.4 适用——推导）；官方 dev/test 磁盘下限 FE 10 GB+/BE 50 GB+ **本轮升格为一手 URL**，§8-R5 偏离登记维持。⇒ 后续实机 task 三张待开（HOUR 演练实机 / DROP 后回盘实测 / compaction×20 GB 对撞），**均须先获批**。
> ⚠️ **编号说明**：D6 是 [08] 落子的「Paimon/Flink/Doris 一手事实」，本条的编号因并发落子后移为 **D7**（先[08]后[04]的顺序）；**D8 是 [07] 落子**（注意与 `gdd.md` 的章节 D8 无关——那个 D8 本次反而被这条推翻了一半）。D1–D8 均为一手论据支撑的推导产物。
  - ⚠️ D1–D5 是本轮 agent 的推导产物，一手论据在 HLD 对应章节与 `canonical-truth.md` 的 file:line 引用，尚未在票里逐条重述。
  - **(D6) Paimon / Flink / Doris 用法的一手事实**（票 [08] 已落子，全文见 [issues/08](issues/08-lake-shaping-and-schema-evolution.md#answer) 与 [research/08a](research/08a-paimon-flink-sql.md)·[08b](research/08b-flink-sql-late-and-early-fire.md)·[08c](research/08c-doris-mow-partial-columns.md)）：**14 条副作用面逐条归档**，其中**八条会改变已有设计**——①**B5 成立**：Flink 2.2 仍无 SQL 级迟到侧输出（FLINK-10031/20527 均 Open/Unresolved，官方原话 "The Table API & SQL drop late events right now."）⇒ gdd D11「混合路线」由文档级断言升为**源码+双 JIRA 现状级**的硬约束，**冲突 C 可从「挂起待证」转正**；②**B6 更强**：唯一支持 early-fire 的 Group Window Aggregation 在 2.2 官方文档标 **`deprecated`**（源码 "will be dropped"），TVF 聚合官方明写 "do not emit intermediate results" ⇒ gdd D12 需补记该事实；③**A3 证伪**：Paimon 层级是 catalog→database→table，**`ecom_lake` 官方对应 database**，`REQ-F-LAKE-06`「单一 catalog（库名 ecom_lake）」一个词混说两件事且判据不具区分力 ⇒ **建议新增冲突 M**；④**A1 证伪**：Paimon 2.1.0 **无 `MERGE INTO` 语句**，官方等价物是 `sys.merge_into` procedure；⑤**A2 部分证伪**：`$files`/`$snapshots` **官方未给完整列名清单**，`$schemas` 无 `schema` JSON 列 ⇒ `research-notes.md` 的列名清单非官方逐字；⑥**A8 补强**：Paimon **主键表默认 MOR 而非 MOW**（MOW 需显式 `deletion-vectors.enabled`），官方 bucket 指引 "200 MB–1 GB per bucket" 明写是 MOR 口径；⑦**C7 收窄**：Doris **普通 `partial_columns` 受 "Batch constraint" 限制**（一批内只能更新同一组列），一批内多列组须用 3.1.0+ 灵活列更新（JSON + `skip_bitmap_column`，且禁用 `sequence_col`）⇒ gdd §1.5 的「`partial_columns=true`」**不是表属性**。⑧**D-11 补强**：**SQL 侧连 allow-lateness 都没有**（FLINK-24000 Closed/Later，逐字 "Currently, Window aggregate does not support allow-lateness like Group Window Aggregate."）⇒ gdd D10 的「允许延迟内→窗口修正」**必须落在 DataStream 段**，`allowedLateness` 是 DataStream-only；⑨**D-14 排除替代路径**：CUMULATE 官方只声明累计语义，**未给出**支持 `early-fire.*`，源码上走 TVF 路径会被直接抛异常。另补：官方迟到机制映射（`allowedLateness` → late firing → "multiple results for the same computation … deduplicate them"；`sideOutputLateData` → "the data that was discarded as late"）为 gdd D10 落点提供依据；`sys.expire_snapshots(retain_max => N)` 为 `REQ-F-LAKE-08` 判据提供可执行手段；`file://` compaction 官方入口为 action jar + `flink run compact`、**成本数字官方未给出**。**版本锚点未动**，不重启选型论证。

## Not yet specified

1. **F1 · 事件 schema 的版本化铁律与演进演练**→ **已由 [01] 落子**（`canonical-truth.md` §3.1：四条铁律 + 六处落点分工）；仅剩「六处落点的逐字撰写」随 [09] 执行。
2. ~~**F2 · 公开数据集回放在首版的范围**~~ → **已由 [05] 收敛**：P2 门槛批由生成器 `dump` 现产（104 万事件 / 3 天跨度，落 `data/` 不入 git）；`REQ-F-LAKE-09` 维持 COULD、公开数据集回放**不在 P2 最小集内**；回放可用性由冲突 H 的相对下界保障（签字版在 `canonical-truth.md` §2.2）。
3. **F3 · 生成器 live 模式的确定性**（挂 [02]）：`REQ-F-GEN-06`（同种子同哈希）在 live 下要同时满足「pin 到补丁号的 Faker」「同种子」「按速率写」——速率是墙上时钟量，三者能否同真说不清。
4. ~~**F4 · 迟到计数表落在哪**~~ → **已由 [05] 收口**：原则（[01] §3.3）＋取值（表名 **`dwd_late_event_count`**、粒度 **`dt` × 60 s 档**、append、只存计数）齐备；FC-2 已批，落笔归 [09]。
5. ~~**F5 · `client` 何时不该存在、空转开销**~~ → **已由 [04] 落子**：`client` 在 **Docker Desktop 面必需、原生 Linux 面可选**（官方「Linux hosts can reach the nodes directly」）；常驻记账用**按需 `run --rm`**，并计入「全部声明上限」口径（10.5 GiB）而非「常驻上限」（5.25 GiB）。见 [`reproducibility-boundary.md`](reproducibility-boundary.md) §4。
6. **F6 · 降级阶梯的触发条件**（挂 [02]）：HLD §8-R6 的 Superset 减服务 A/B/C、gdd §1.11 的 G4（Kafka 原生镜像）都没写「什么信号出现才降级、降级后谁回填实测」。
7. **F7 · 世界时钟**（挂 [02]）：`--seed` 下的回放会带 2022-2024 的时间戳，而 1 分钟窗口按「当前水印」关窗——回放怎么被当成实时喂进去，涉及是否允许写 event_time 而不是墙钟。
8. ~~**F8 · 多 JDK / 多 Python 的现实包袱**~~ → **已由 [04] 收敛为一句可判的话**：**JDK/Maven 只服务于「构建 Flink 作业」，不在起链路路径上**（`up`/`init`/`verify`/`lint`/`typecheck` 都不需要它）⇒ 它既不挡 L0，也不进 `REQ-F-ORCH-01` 的判据；`uv` 的三处偏差属生成器施工面，归 **P1**。见 [`reproducibility-boundary.md`](reproducibility-boundary.md) §6。

## Out of scope

- **本机环境的一次性动作**（Docker Desktop 换盘/重装、全局网络设置、宿主 JDK/Maven 面调整）：属环境事实，已在 HLD §4 落定。
- **`docs/adr/` 的建立**：不在仓库时静默继续；本图不产 ADR。
- **推送与 GitHub Issues**：本仓 issue 后端是本地 markdown；`deck_*` 的 GitHub 后端不适用，命中即停。
- **面试材料与简历 bullet 的成稿**：P3 的一部分，但不属 spec-ready 判据。
- **亿级回放的实际执行**（`REQ-NF-CAP-06`）：只定口径，不排期。
- **Spark 离线对照 profile 的实现**（`REQ-F-ORCH-08` 的一半）：只留判据。

## 为什么 [01] 已经先做掉了（历史记录，2026-10-20）

1. **它是根**。`[01]` 定「谁该让路」；不定它，`[03]` 里「行为可证」的判据、`[05]` 里 schema 的形状、`[08]` 里 MoW 部分列更新，都在各自定义各自的口径——这正是本仓历史上最大的坑（同一件事在三个文件里有三个说法）。
2. **它清掉的迷雾最多**。F1 已并入它；F4、F7 挂在它定下来的「落点」与「时钟」口径上。
3. **它最省事**。9 张票里，它的答案是一张**口径表**，不需要碰任何一台机器；而 `[04]` 要动交接文档与体积口径，先做它会产出一堆随后被 `[01]` 改写的措辞。
4. **反方意见**：[04] 是「立项基石」的延伸（可复现），且是纯 AFK 友好（可在我这边跑完合流，你只签字）。若你更想先把「可复现」这件事钉死，就先做 [04]——代价是 [01] 之后仍要回头改一遍它的措辞。**这一取舍我没有替你决定。**

> ✅ **2026-10-20 用户裁决：先做 [01]**，理由取第 1、3 条。它已落子，产出 [`canonical-truth.md`](canonical-truth.md)。
> ✅ **2026-10-20 后续：**[08] 与 [04] 已相继落子（[08] ⇒ D6 + `research/08a-c`；[04] ⇒ D7 + [`reproducibility-boundary.md`](reproducibility-boundary.md)）；**[07] 已落子**（⇒ D8 + [`research/07-doris-dynamic-partition-retention.md`](research/07-doris-dynamic-partition-retention.md)：判据定为「配置＋小时级演练」；gdd D8 的 FE 总开关前置条件与 D3 的 6 小时算术**被推翻，修订交 [09]**）。
> ✅ **2026-10-20 再后续：[05] 已落子**（⇒ **D9**，18 问全按推荐：强序＋判据分层、dump 门槛批、单目标 `replay`、五层形状表、快照 [5,20]、不变量改三条断言、全 SQL 零 jar、warehouse 卷硬约束；**[03] 解除阻塞**；F2/F4 收口）。**[05] 的 5 项批准与 6 条落笔分工见票 Answer 末节，全部归 [09] 汇聚。**
> **现在的前沿：[03]（grilling，唯一可开）**——[06] 等 [03]；[09] 等其余全部。
>
> ~~⚠️ **动笔前的必读指针（[04] 用，已被执行完毕）**~~ —— 保留作留痕：
> - ~~**必读 ①**：`canonical-truth.md` **§1 冲突 I**（内存双口径的逐字新句）~~ → [04] 已按它写 HLD §4.2 的体积表与两口径说明。
> - ~~**必读 ②**：`canonical-truth.md` **§4.3 F-1**（10.25/1.43 → 10.5/1.18）~~ → ⚠️ **仍未落笔**：按裁决归 **[09]**；[04] 已在 HLD §4.2 加指针并要求「引用一律用 10.5/1.18」，**没有改那个数字**（避免第二真相源）。
> - ~~**G1 的适用范围**~~ → [04] 已按此执行：`REQ-F-ORCH-01`、`REQ-NF-CAP-03`、`REQ-NF-CAP-04` 三处**均按意图类走了签字**（2026-10-20 已签），逐字新句写在 `reproducibility-boundary.md` §8，落笔归 [09]；体积口径按事实类就地改。
