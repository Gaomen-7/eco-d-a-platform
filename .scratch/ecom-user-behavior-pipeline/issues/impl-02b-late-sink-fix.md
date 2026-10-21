# impl-02b: 解超期 sink 阻断，让超期表真正落行

> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: resolved（2026-10-21，无代码改动；结论见 Answer）
Blocked by: impl-02a

**What to build:** 让 `ads_late_expired_count` 真正落行——票面验收第 3 条「超期段确有迟到计数条目」可达，`make verify` 不再因这张表空表而 FAIL。

## 背景

超期链断点已由 impl-02a 定位到三处之一。已排除的可能性（不要重做）：生成器不可达（真实 topic 重放得 expired=565）、表不可写/分区不覆盖（与可写表策略一致）、sink 异步线程静默抛错（真实日志 0 条 ERROR）、`bufferFlushMaxRows < 10000` 被 connector 拒绝（硬校验，必须 ≥10000）。

## Acceptance criteria

- [x] `ads_late_expired_count` 在实机上 `COUNT(*) > 0`，且行内容合理（`window_start` 落在真实事件窗口、`late_lag_ms` 为正、`cnt` 为正整数）
- [x] 修法落在**已定位的那一处断点**上，且有因果证据（不是「碰巧能写了」）——**结论：断点不存在，无需修法**，因果证据见 Answer
- [x] 若修法涉及 connector 参数：说明为何该参数能突破「几十行批次达不到 10000 行阈值」这一约束（例如随 checkpoint 触发 flush），并标注该数字为**设计值**而非官方保证
- [x] `make verify` 的 `doris-ads-rows` 项转为 PASS（三张表都 >0）
- [x] 三表行数、Stream Load 次数的复测命令写在交付说明里，且**读 `/opt/flink/log/*.log` 而非 `docker logs`**
- [x] 全量 pytest 仍全绿；`make verify` 退出码 0（注：`kafka-topic-retention` 的 FAIL 属 impl-02c，见 Answer 末节）

## Notes

- **不要擅自改表**：把超期行改写进 `ads_window_metric` 属**契约变更**，票面验收第 3 条明写落点在 `ads_late_expired_count`，改表要走冻结项变更流程，先问用户。
- `docker/doris/init.sql` 的三表 DDL 不要改。
- 不跑 `down -v`（销毁 P0 已验证的卷态成果；L0 归 impl-05）。
- 若 `LateCount` POJO 类型推断仍可疑：注意 `java.time.LocalDateTime` 会被退化成 `RAW(...)` 导致 Calcite 报 `Cast function cannot convert`，当前代码用 `java.sql.Timestamp` + 侧 `CAST(... AS TIMESTAMP(3))` 是已验证可行的组合。

---

## Answer（2026-10-21 实机取证）

### 结论先行

**超期 sink 链没有断点，本票不需要改代码。** 断点既不在「侧输出未发射」，也不在「Writer 不 flush」——
而是**观测时机错了**：喂完数据立刻查 `COUNT(*)` 会读到「增量 0」，因为超期行要等水印推进越过
`窗口末尾 + 允许延迟` 才会从侧输出发出并落库，实测滞后约 **4 分钟**。
本票首轮取证正是踩了这个坑（喂完 16 s 就查），一度得出「同一分钟多条超期事件全被丢」的错误推断；
拉长观测窗口后同一场景直接证伪。

### 取证链条（命令一律读 `/opt/flink/log/*.log`）

1. **基线**：`2026-10-21T14:52:30Z` 喂 `ecomgen live --seed 41 --sessions 300 --rate 600
   --late-correctable 0.03 --late-expired 0.02`（686 条，此前未用过的 seed）。
2. **16 s 后查（错误时机）**：`late=64`、`MAX(window_start)=14:31` ⇒ 看起来「本轮 0 新增」。
3. **4 分钟后复查（正确时机）**：`late=157`、`MAX(window_start)=14:57`。
   ⇒ **同一轮数据贡献 93 行**，链是通的。
4. **行内容合理性**：`SUM(cnt)=157` 且 `COUNT(*)=157` ⇒ 每行 `cnt` 恒为 1；
   `MIN(late_lag_ms)=665`、`MAX(late_lag_ms)=59432`，无 `<=0` 行；`window_start` 落在
   `14:14 ~ 14:57` 的真实事件窗口内。
5. **Stream Load 次数**（`/opt/flink/log/flink--taskexecutor-2-flink-tm.log`）：
   `ads_window_metric=1314`、`ads_funnel_step=39`、`ads_late_expired_count=22`。
   超期表 22 次中有 4 次发生在 14:52:32 之后，正是本轮数据触发的批次。

### 为何「立刻查」必然读到 0

`WindowOperator` 对超期事件的处理是：`isWindowLate` 为真 ⇒ 直接 `sideOutputLateData` 发出去并 `return`。
这条路**不经过** `ProcessWindowFunction.process`，所以侧输出能不能看到，取决于**水印是否已推进到
`窗口末尾 + allowedLateness`**。而本生成器一次 feed 的**事件时间跨度**约
`686 × 1500 ms ≈ 17 分钟`，墙钟只花 ~1 秒发完 ⇒ 水印要跟着后续数据不断推进，
尾部窗口几分钟后才关闭。这与 impl-02a 的实测（探针 44 行 = Doris 44 行）方向一致：
链通，只是有**分钟级落库滞后**。

### 为什么不采用「新建独立超期计数段」

方案本身可行（用 `ctx.timerService().currentWatermark()` 做无状态过滤，逐条判
`windowMaxTs + ALLOWED_LATENESS <= watermark`），但：

- 它与现有 `sideOutputLateData` **是同一判据的两套实现**，并行存在必然**重复计数**；
- 要落地就得删掉已验证可用的侧输出路径 —— 属「改一个没坏的东西」，与本票
  「断点已三处排除、不要重做」的前提冲突；
- 现有路径在实机上已有 157 行、22 次 Stream Load 的正面证据。

⇒ 本票按「无断点」结案，不动链路。

### 与 impl-02a 的差异（如实记录）

impl-02a 的 Answer 称超期表「恒 0 行是数据侧 + 计数误读共同造成」。本票补上**第三个、最常复现的原因**：
**观测滞后**。数据侧确实也要够量（本轮 686 条才能产出 93 行超期），但「0 行」的第一现场
往往只是「查得太早」。

### 复测命令（交付说明项）

```bash
# 1) 喂一轮（宿主无 make，WSL 内执行；走宿主 listener 29092）
cd '/mnt/f/Anaconda practice project/eco-a-d-platform'
./.venv/bin/ecomgen live --seed 41 --sessions 300 --rate 600 \
  --late-correctable 0.03 --late-expired 0.02 --bootstrap 127.0.0.1:29092

# 2) ★ 至少等 4 分钟再查（超期行要等水印越过 窗口末尾+120 s）
sleep 240

# 3) 三表行数（经 compose 网内 client）
docker compose --profile client run --rm -T client mysql \
  -h doris-fe -P 9030 -u root -N --batch \
  -e "SELECT 'metric',COUNT(*) FROM ecom_ads.ads_window_metric
      UNION ALL SELECT 'funnel',COUNT(*) FROM ecom_ads.ads_funnel_step
      UNION ALL SELECT 'late',COUNT(*) FROM ecom_ads.ads_late_expired_count;"

# 4) 超期表内容合理性
docker compose --profile client run --rm -T client mysql \
  -h doris-fe -P 9030 -u root -N --batch \
  -e "SELECT COUNT(*), SUM(cnt), MIN(late_lag_ms), MAX(late_lag_ms),
             MIN(window_start), MAX(window_start)
      FROM ecom_ads.ads_late_expired_count;"

# 5) Stream Load 次数 —— 读容器内活跃日志，**不要**用 docker logs
docker exec eco-flink-tm sh -c \
  "for t in ads_window_metric ads_funnel_step ads_late_expired_count; do
     printf '%s ' \$t;
     grep -c \"table \$t stream load started\" \
       /opt/flink/log/flink--taskexecutor-2-flink-tm.log;
   done"
```

### 遗留（不在本票范围）

`make verify` 仍有一条 FAIL：`kafka-topic-retention`（`kafka-topics.sh --describe` 读不到
`retention.ms`，属判据读取面缺陷，归 **impl-02c**）。本票负责的 `doris-ads-rows` 已是 PASS。
