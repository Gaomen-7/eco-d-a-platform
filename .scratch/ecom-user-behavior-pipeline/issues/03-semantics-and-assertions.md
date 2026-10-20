# 03 · 语义与判据：漏斗/UV 的窗口语义 + 判据强度 + 「行为可证」的统一断言形式

Type: grilling
Status: open
Blocked by: 02, 05
Blocked by (note): `02` 指**地图票 D4（已落子）**；`05` 是真实票号（[离线分层范围](05-offline-layer-scope.md)）。
Created: 2026-10-20（原 05 + 09 合并）
Ticket: 03
Source: map.md · GLOSSARY「转化漏斗」「滚动 UV」「落定值/暂定值」· gdd §1.3 / §1.8 · SRS §3.1.8 · HLD §6

## Question

这张票回答一个问题：**「这个量算对了没有」在实时链路上怎么写成一个断言。** 两半：同一批数字既要有语义，也要有判据。

### A · 语义：三条时间轴在同一个 Doris 表里

`GLOSSARY.md` 立了硬区分：漏斗的 1 分钟窗口只决定「每分钟统计多少条步进」，漏斗本身按 `user_id` 步进推进、与窗口正交；滚动 UV 是「最近 1 小时去重用户数」。于是 `ecom_ads` 里至少三个时间轴并行：

| 量 | 时间轴 | 状态规模 | 谁在算 |
| --- | --- | --- | --- |
| 每分钟 PV / GMV / 品类·地域分布 | 1 分钟滚动窗口 | 窗口态 | Flink SQL 窗口聚合 |
| 滚动 UV（最近 1h） | 1 小时滚动 | 键控去重 ~MB 级 | Flink SQL / DataStream |
| 四步漏斗 | 会话（30 min 空闲切断）+ 窗口只做客数 | 键控状态 TTL 32 min | DataStream（`autofill` + 双列并陈） |

1. **多行语义**：`revision` 递增的覆盖写只发生在窗口聚合行，还是漏斗行也会被迟到事件修正（一条 `purchase` 迟到 90s，算不算修正了某分钟的漏斗客数）？
2. **提前发射**（gdd §1.3-4）：每 10s 的「暂定值」与 1 分钟落定值，是同一张表的同一行（靠 `revision`/状态列区分），还是两张表？两张表的代价是唯一键不同，gdd D1 的「一层主防」前提要不要重证。
3. **对账口径**：`gdd §1.4` 要「实时滚动 UV 与离线精确 UV 逐窗口相等」；离线是 Paimon 批，窗口切分必须与实时完全一致才算得出相等——口径写在哪、谁维护。
4. **补记与单调**：`--chaos-ratio` 下 `funnel_cnt_observed` 可非单调、`funnel_cnt_inferred` 必须单调；看板同屏展示两列时，唯一键还包不包 `revision`。

### B · 判据：`verify` / `verify-fault` 的退出码写到哪一层

`gdd §1.8` 给了五项烟测的确切断言（`jobs[].state == "RUNNING"`、`/health` 纯文本 `OK`、`count(*)` 而非 `total_record_count`…），但 **`verify-fault` 只有「条数一致」「脏率不上升」两句自然语言**；gdd §1.3 的四条行为可证判据与 §1.4 的不变量断言**是否进入退出码**也没人说。

5. **退出码语义**：`verify` 退出 0 表示「五项全过」，还是「五项 + 幂等 + lag + 不变量 + 脏率」全过？
6. **`verify-fault` 的机器判据**：kill TM → 等 RUNNING → 比对 Doris `count(*)`；「等多久」、比对**哪张表**、「脏率不上升」的分子分母各取什么。
7. **负断言怎么写**：`gdd §1.8` 的「条数一致但脏率飙升 = 不通过」——基线脏率从哪来。
8. **`lint`/`typecheck` 的位置**：HLD §3.3-6 明确不混入 `verify`；将来要不要一个 `make check` 总入口（要，就是冻结项变更）。
9. **fail 的痕迹**：断言结果落 stdout 行、JSON 文件、还是 Flink REST 原始响应快照（横切关注点 2「失败必须有痕迹」）。

## 约束

- HITL。可以直接起一个 **AFK 原型**（单文件 HTML，照 `prototypes/funnel.prototype.html` 的方式跑断言）把「1h UV × 1min 窗口 × 30min 会话」叠加起来看——原型是手段，裁决仍是你。
- 不改 `GLOSSARY.md` 已定义的概念；有冲突就在 Answer 里提议修订词条。
- 每条判据裁定都要给**脚本可执行的形式**（命令 + 解析方式），否则等于没定（HLD §0.4）。
- 不改八个冻结目标名；加 `make check` 要走冻结项变更，并在 Answer 单列「需你批准」。

## Answer

<!-- 待解。解析步骤：Skill "prototype"（A 部分可选先做）→ Skill "grilling" + "domain-modeling" → Status: resolved → 往 map.md 追加 -->
