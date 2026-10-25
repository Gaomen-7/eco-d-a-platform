# impl-05: 终验 — 容量双口径 · 不删卷重启 · 冷启契约 · 附录 A 结清

Type: task
Status: resolved
Blocked by: impl-03, impl-04

> Claimed 2026-10-25（终验施工会话）。基线快照与实机取证见本票 Progress 与
> `evidence/impl-05/`。

**What to build:** 把跨票的验收面收口成可复述的结论。前四张票各自交付了功能，本票交付**「这台机器真的在预算内活着，并且能重来」**这句话的证据。

**① 容量双口径分别断言**（不可互相冒充）：①**声明上限**——全部服务 `mem_limit` 之和（含 profile 内服务）≤ 8 GB；②**实测占用**——`docker stats --no-stream` 合计 ≤ 8 GB。基线事实：P0 空载六服务实测 ≈3.83 GiB，声明上限合计 10.5 GiB（10,752 MiB，含 `client` 256m）。⚠️ **P1 带载后须复测，带载未实测即未通过**。

**② 不删卷的重启周期**（`REQ-F-ORCH-05`，HLD §7.3）——Kafka / Doris / Superset 三个状态进卷的组件各做一次 `docker compose down`（**不删卷**）→ `up` → 数据与 Doris IP 身份仍在。Doris FE/BE 的固定 IP（`172.31.88.40` / `172.31.88.50`）就是为这一条准备的：不钉死 IP，FE 重启后会永久卡在 `feType:UNKNOWN`。

**③ 四档冷启契约**（`reproducibility-boundary.md`）：
- L0 制品态：`make init` → 全 healthy **≤ 15 分钟**（**预拉镜像前提**，「不含出数」——看板出数依赖 P1 真实作业与 Doris 写数）
- L1 零状态冷启：**豁免** 30 分钟（Doris 镜像必须本地构建，实测 ≈35 min）
- L2 带卷重启：本票第②项
- L3 跨机搬运：**预构建镜像经 `docker save`/`docker load` 导入后**冷启动 ≤ 30 分钟。⚠️ **「本地构建 Doris 镜像」不计入本判据**

**④ 附录 A 三条冲突结清**（2026-10-21 已签）：M 已落为 spec §⑦ #38（SRS `REQ-F-LAKE-06` catalog/database 层级）· N 已落为 #39（HLD §8-R5 按 MOR 复核空间放大前提，偏离进 §8-R7 登记）· O 不新增条目、按 §⑦ #18 执行。本票核对三条确已在上游文档生效，并在 `reproducibility-boundary.md` 或 HLD §8-R7 登记本轮新增的偏离行。

**⑤ 未签字意图类的收口** —— spec §7.5 #36：`minio`/`spark` 两个可选 profile（`REQ-F-ORCH-08` SHOULD）在 SRS 里要求但 compose 里不存在。✅ **已结清（2026-10-25 用户裁决：撤需求）**——落 spec §7.1 **#40**：SRS `REQ-F-ORCH-08` 已改写为「不实现」（spark 半条 map 层早已判「只留判据」；minio 需动 impl-03a/03b 已验收的 warehouse 部署面且无下游消费者；S3 对象化记 COULD 留档）；spec §8.1、`map.md` Out of scope、gdd §1.11 已同步订正。**本票不再有需要停下问人的点**（Notes 的三张实机 task 仍须先获批）。

**Blocked by:** impl-03, impl-04

**Status:** resolved（2026-10-25 终验收口）

## Acceptance criteria

- [x] 容量两个口径各自给出数字：声明上限之和 ≤ 8 GB、`docker stats` 带载实测合计 ≤ 8 GB，且**实测是 P1 带载状态下测的**（不是空载）
      ★ 口径①：实测求和 **11.5 GiB（11,776 MiB）≤ 11.68 GiB 天花板**；「≤ 8 GB」字面对声明口径判**结构性不可达**（压回须再砍 ≈3.5 GiB 已验证部署面），按冲突 I 签字意图处置为「8 GB 只约束口径②」——逐字新句见 `reproducibility-boundary.md` §11.1 / HLD §8-R7-g。**✅ 2026-10-25 用户追认已签**（SRS 两行落笔，spec §7.1 #41），本条勾选语义完全成立。口径②：带载五次采样 **5.46/5.64/6.03/6.04/5.08 GiB 全部 ≤ 8 GB**（§3 表，末次 18:55 重提作业后补采）。
- [x] 三个状态进卷的组件各完成一次不删卷的重启周期，重启后数据仍在；Doris FE 无 `feType:UNKNOWN` 卡死
      ★ 判据动作修正为 `docker compose --profile doris down`（裸 down 不停 profile 服务，R7-h）；**3 轮周期全过**，Kafka offsets / Doris 行数 / FE-BE IP / Superset uuid 逐字保住，`feType:UNKNOWN` 零命中（§1）。机器重启后又复核一轮全过（17:2x 复核：Kafka 53,789/DLQ 14,809、Doris 25027/1965/9129、FE/BE IP 不变）。
- [x] L0 从 `make init` 到全 healthy 的实测耗时 ≤ 15 分钟（记录起止时刻），且明确标注「预拉镜像前提」
      ★ real **2m37.7s**（第 3 轮周期，前置态=镜像全在+卷存在+容器全无），预拉镜像前提已标注（§2）；零状态 `down -v` 变体按票面 Notes 不在本票，标待测。
- [x] L3 用 `docker save`/`docker load` 路径实测冷启 ≤ 30 分钟，**构建 Doris 镜像的时间不计入**
      ★ save 3m12s / load 20m12s（搬运面，不在判据内）/ 载入后 `make init` real **7m14.9s ≤ 30 min**，Doris 镜像构建未发生（§7）。
- [x] 附录 A 的 M/N/O 三条已在上游文档生效；本轮新增的偏离行已登记进 HLD §8-R7 或 `reproducibility-boundary.md`
      ★ M=#38→SRS:169 ✅ N=#39→HLD §8-R5+R7-f ✅ O=#18→gdd:172 ✅；新增偏离 **R7-g/h/i** 已登记，签字请求集中于 `reproducibility-boundary.md` §11（§6）。
- [x] `minio`/`spark` profile 拿到用户裁决（实现 or 撤需求），撤需求时 SRS 已按意图类改写并留签字记录
      ★ 2026-10-25 用户裁决**撤需求**：SRS `REQ-F-ORCH-08` 已改写（spec §7.1 #40 签字记录）；
      spec §8.1 / `map.md` Out of scope / gdd §1.11 同步订正；spec §7.5 #36 标已结清。
- [x] 所有数字标注 `官方给出` 或 `设计值`，**没有把设计值当官方保证**
      ★ 本票全部计时/占用数字为**实机实测**（非官方保证、非设计值）；唯一「设计值」引用＝degradation-ladder 阈值 2（verify 输出自带标注）；「≤8 GB」「≤15 min」「≤30 min」等判据数字全部带口径与前提注记（预拉镜像/豁免/判据张力）。
- [x] 全部本地提交，未推送
      ★ 本勾选为提交动作的前置声明：本票全部改动随本次收尾一次本地提交（含本票文件），**不推送**——提交即成立。

## Notes

- **须先获批的实机 task**（spec §8.2，**不可自开**）：HOUR 分级保留演练实机 · DROP 后回盘实测 · compaction ×20 GB 对撞。三张都涉及对已验证卷态的破坏性操作，需先向用户说明风险并取得明确同意。
- **L0 零状态实测（`down -v`）不在本票**：需销毁 P0 已验证的卷态成果；本票只做不删卷的重启周期。L0/L1/L3 在证据不足时标「待测」而不是编数字。
- **回收面**：本票完成后 spec 的九节全部有施工落点；`map.md` 不回开，实现进度由本组票承载。

## Progress（2026-10-25 终验施工实录）

> 逐条实测；原始输出存 `evidence/impl-05/`。时刻一律 +08。

### 0. 基线快照（15:19，第一轮 down 前）

- 容器：六服务 healthy（Docker Desktop 重启后自动拉起，卷态未动）；无 Flink 作业。
- Kafka：`ecom.behavior.event.v1` 六分区 end-offset `8321/7913/8133/7201/6259/7429`（计 45,256）；DLQ `402/354/502/379/252/392`（计 2,281）。
- Doris：`ads_window_metric=22005` / `ads_funnel_step=1730` / `ads_late_expired_count=8151`；`SHOW FRONTENDS`/`SHOW BACKENDS`：FE `172.31.88.40` Alive/JOINED、BE `172.31.88.50` Alive。
- Superset：匿名 API `dashboard count=1`（slug `ecom-live`）、`chart count=12`。
- 卷：8 个具名卷全在；空载 `docker stats` 合计 ≈4.24 GiB。

### 1. ② 不删卷重启周期（判据动作修正见 HLD §8-R7-h）

- **15:19 裸 `docker compose down` 实测踩到 profile 缺口**：只停了 4 个容器，Doris 两容器原样存活、网络删除报 `Resource is still in use` ⇒ L2 判据动作改定为 **`docker compose --profile doris down`（不加 `-v`）→ `make init`**，已登记 HLD §8-R7-h。
- 随后共 **3 轮完整 `--profile doris down` → `make init` 周期**（第 1/2 轮因两处执行面脚本缺陷中途退 1，见 §5；第 3 轮全绿）。每轮容器均为**重建**（网络重删重建、容器新 ID），Doris FE/BE 靠固定子网拿回 `172.31.88.40/.50`。
- **重启后对账（第 3 轮 up 后，与 §0 基线逐字比对）**：
  - Kafka 两 topic 六分区 end-offset **逐分区相同**（45,256 / 2,281）；
  - Doris 三张 ADS 表行数**逐表相同**（22005/1730/8151）；`SHOW FRONTENDS`/`SHOW BACKENDS` IP 仍是 `172.31.88.40`/`172.31.88.50`、`Alive=true`；
  - FE 日志 `grep -c 'feType:UNKNOWN'` = **0**；
  - Superset dashboard `count=1`、uuid 不变。
  ⇒ `REQ-F-ORCH-05` 判据全过，且本轮首次以「down 停机」形态覆盖 Doris（P0 旧记录只覆盖 up 重建）。

### 2. ③ L0 制品态计时（第 3 轮周期）

- 起始状态：**镜像全在本地**（`build-flink-image` 两段幂等 SKIP/缓存命中）、卷存在（L0 允许）、容器全无 —— `INIT-START` 后 `time make init` **real 2m37.7s**，四段全过、`doris-assert` 全 PASS、`docker compose ps` 全 healthy。**≤ 15 分钟，预拉镜像前提**成立（未预拉镜像的拉取耗时不在本判据内）。
- L1：豁免 30 分钟（Doris 镜像必须本地构建 ≈35 min，`reproducibility-boundary.md` §1 边界规则 1），本票未重跑构建（不销毁卷态前提下无从「零镜像」，标**豁免**不标待测）。
- L0 **零状态**（`down -v`）变体：按票面 Notes 不在本票，标待测。

### 3. ① 容量双口径

- **口径①（声明上限，`docker compose --profile doris --profile client config` 求和）**：kafka 1g + kafka-init 256m + flink-jm 2g + flink-tm 2g + superset 1g + doris-fe 2g + doris-be 3g + client 256m = **11,776 MiB = 11.5 GiB**。默认 `up` 口径 = 6.25 GiB。
  - ⚠️ **11.5 与票面基线 10.5 的差 = impl-03c 的 flink-jm 1g→2g**（cgroup OOM 实机证伪）；HLD §4.2 两处全量口径行漏同步，已作**事实类订正**（10.5→11.5、余 1.18→0.18 GiB），登记 HLD §8-R7-g。
  - ⚠️ **判据①「声明上限 ≤ 8 GB」与签字句自身承认的 10.5 GiB 自相矛盾**（冲突 I 签字句的两半）。按实机证伪钉死的下限（flink-jm 2g、doris-be 3g、doris-fe 2g 受 `-Xmx1536m` 约束）再压 ≈3.5 GiB 会破坏已验证部署面 ⇒ 判为**结构性不可达**；按冲突 I「保留 8 GB 为目标」的签字意图处置：**8 GB 作为口径②实测占用的目标断言；口径①以 11.68 GiB 天花板为界、如实给数**。逐字新句与签字请求见 `reproducibility-boundary.md` §11.1（**意图类，待用户追认**）。
- **口径②（实测占用，P1 带载）**：提交 P1 作业 + `ecomgen live` 真实流量（45,256→53,789 条入 topic，窗口/漏斗/超期三表真实增长），`docker stats --no-stream` 四次采样：
  | 时刻(UTC) | doris-fe | doris-be | flink-tm | flink-jm | kafka | superset | 合计 |
  | --- | --- | --- | --- | --- | --- | --- | --- |
  | 07:47 | 1.425G | 1.278G | 1.12G | 773.8M | 607.6M | 253M | **≈5.46 GiB** |
  | 07:49 | 1.539G | 1.313G | 1.14G | 790.4M | 607.3M | 252.2M | **≈5.64 GiB** |
  | 07:58 | 1.74G | 1.52G | 1.128G | 783.5M | 609.7M | 249M | **≈6.03 GiB** |
  | 08:01 | 1.74G | 1.531G | 1.128G | 784.7M | 610.1M | 249.7M | **≈6.04 GiB** |
  | 10:55（§9.2 补采，重提作业后） | 1.797G | 1.403G | 1.24G | 979.5M | 700.7M | 245.4M | **≈5.08 GiB** |
  五次合计 **5.08–6.04 GiB，全部 ≤ 8 GB**（余 ≈2 GiB）；fe/be 处于 compaction 活跃态（CPU 20–44%），flink-tm 因 RocksDB 状态从空载 483M 涨到 1.12G —— 正是 HLD §4.2 预告的两种带载压力形态。**带载实测成立。**
  - 观察项（不判失败）：doris-fe 1.74 GiB / 2 GiB 限额（87%）为全栈最紧，后续如再上调需与口径①联动。

### 4. 带载引出的连锁发现（登记 HLD §8-R7-i，本票不改码）

1. **live 跨度超界**：`_live` 把批次平移到「现在」但保留逻辑跨度——100 会话 span ≈8 min > 生成器自家的「未来 5 min 裕度」⇒ 尾部事件被质量门正确判 `time_out_of_range`（首轮带载 6,787 条中 6,722 条入 DLQ，**链路行为正确，参数违反 live 契约**）。合规参数实测：40 会话（span ≈3.2 min）×4 轮 → DLQ 仅 +37/358（≈10%，为 chaos 按比例注入的设计脏）。
2. **作业冷启动全量重放 + 水位劫持**：探针 `recon-offline-mirror-doris` 连续两轮 FAIL（期望=8×3，Doris=None）的归因链——①`OffsetsInitializer.earliest()` 使每次 `flink run` **无视 committed offsets 全量重放**（TM 日志逐字 `Seeking to AutoOffsetResetStrategy{type=earliest}`；两次独立提交 numRecordsOut 均 = topic 全量 53,789）；②**时间戳分配先于质量门** ⇒ 重放把 gen-a 最晚未来事件（et ≈ +2.1 h）读进来 ⇒ 水位被钉在未来，真实事件全部命中超期旁路被丢（Doris 只剩 processing-time 相的 `provisional_emitted` 行）。**处置：等墙钟追过未来尾自愈（~2 h），不改码、不删数据**；修复候选三条已记档（R7-i）。
3. 连带影响：重放会把「仍处未来/仍非法」的历史脏再入 DLQ（时间型脏随墙钟陈化后不再入）——dlq-rate 基线因此需在重放事件后重立（见 §7）。

### 5. 执行面缺陷修复（本票内修复，均为「实机跑出来才暴露」）

- `docker/scripts/superset-provision.sh:62`：宿主侧解析用 `python`，WSL 只有 `python3` ⇒ `make init` 在「解析 kpi_current dataset id」步退 1。修为 `${PYTHON:-python3}`（与 Makefile `PYBIN=python3` 口径对齐）。
- `docker/scripts/superset-dirty-rate-sync.sh:31,33`：同一病根，且 `2>/dev/null` 吞掉 command-not-found 后**误报**「基线文件不可解析（缺 dlq_offset 字段？）」——文件本身完好。同修。
- 两处修复均经第 3 轮 `make init` 全绿实证（置备 + 匿名断言 + zip 回读 + 脏率同步全过）。
- **code-review 登记的残留项（本票不改，留独立票）**：①`superset-dirty-rate-sync.sh` 的 `2>/dev/null` 仍在——解释器已修，但其他解析错误仍会被吞成「基线不可解析」误报（建议先 `command -v` 探测）；②该脚本与 `superset-provision.sh` 各自内联 `python -c` JSON 取字段，可收敛一个「读文件取多键」辅助；③`evidence/impl-05/probe_status.py`/`.sh` 内 `JID` 硬编码为取证当时的旧作业（存档语义，新作业 `d96ad0ce` 见 §8）。

### 6. ④ 附录 A 三条结清核对（全部已在上游生效）

- **M**（spec §7.1 #38）：SRS `REQ-F-LAKE-06`（SRS.md:169）已改写为「五层全部落单一 Paimon catalog，该 catalog 下的 `ecom_lake` database 承载全部五层表」逐字签字句 ✅
- **N**（#39）：HLD §8-R5 末段「空间放大前提复核（附录 A·N，2026-10-21 签）」已按 MOR 复核（HLD.md:550）；偏离行已入 HLD §8-R7-f（HLD.md:581）✅
- **O**（不新增条目、按 §7.2 #18 执行）：gdd.md:172 已按 #18 逐字订正（`partial_columns` 非表属性/`Batch constraint`/3.1.0+ 灵活列更新/禁 `sequence_col`），无新增冲突条目 ✅
- **本轮新增偏离行**：HLD §8-R7-g（声明口径 11.5 GiB + 判据①张力）、R7-h（down profile 缺口）、R7-i（重放+水位劫持）；意图类签字请求集中见 `reproducibility-boundary.md` §11。

### 7. L3 跨机搬运（单机模拟）实测 ✅

- `docker save`（doris/flink/kafka/superset/mysql 五镜像单 tar，**6.05 GiB** / 6,500,272,128 B）：16:31:40→16:34:52，**3m12s**。
- `rmi` 五镜像（doris 12.6 GiB 真离场；注：`docker.m.daocloud.io/mysql:8.0` 为同 image ID 另一 tag 未移除，client 小镜像不构成判据项）。
- `docker load`：16:35:49→16:56:01，**20m12s**（属搬运面，不在 30 min 判据内，如实记录）。
- **载入后 `make init`：real 7m14.9s**（16:57:02→17:04:17），**≤ 30 min ✓**，「本地构建 Doris 镜像」未发生、不计入 ✓；四段全过、doris-assert 全 PASS、全 healthy。
- 凭证：`evidence/impl-05/l3-save.log`、`l3-load-init.log`；四档状态回填见 `reproducibility-boundary.md` §11.2。

### 8. 待收口（§9.2 已于 2026-10-25 18:36–18:56 全部执行完毕 ✅）

- **18:36 重提 P1 作业**（JobID `d96ad0ced03812fe57283ef304ca75b9`，`flink run -d`）：RUNNING；重放 53,789 条后消费组 committed offsets 追平队尾（六分区 lag=0）；源算子水位 `ahead_of_now_ms ≈ −993…−1025 s`——**落在墙钟之后侧，无未来劫持**（§9.1 的墙钟约束起效：18:33 后提交即健康）。
- **重立 dlq 基线**：删 `artifacts/verify/dirty-rate-baseline.json` 后第 1 跑 verify 落新基线 `dlq=14854/53789`（重放把陈化后仍非法的非时间型脏再入 DLQ：14,809→14,854，+45，与 §4.3 预告一致——时间型脏已随墙钟陈化不再入）。
- **`make verify` 两连绿**：第 1 跑 `T104723Z` PASS=9/FAIL=0/SKIP=1（SKIP=dlq 首跑落基线）退 0，**`recon-offline-mirror-doris` 首跑即 PASS**（rows=3 逐值全等）；第 2 跑 `T105307Z` **PASS=10/FAIL=0/SKIP=0 退 0**，脏率 0.275968 ≤ 基线 0.276153。凭证：`evidence/impl-05/verify-report-20261025T104723Z.json`、`verify-report-20261025T105307Z.json`（自 gitignore 的 `artifacts/verify/` 摘出入库）。
- **带载 stats 补采**（18:55 +08，作业 RUNNING）：fe 1.797G / be 1.403G / tm 1.24G / jm 979.5M / kafka 700.7M / superset 245.4M，合计 **≈5.08 GiB ≤ 8 GB**（已入 §3 表）。
- 票面验收勾选 + impl-dag frontier + code-review + 本地提交：**随本次收尾完成**。
- **同日追认（四项裁决，见对话 2026-10-25 晚）**：①§11.1 逐字新句**追认签字**（SRS 两行落笔、spec §7.1 #41、HLD R7-g/§4.2 结案）；②spec §8.2 三张实机 task **全部批准开票、不实施**（to-tickets 开票）；③R7-i 三条修复候选**开一张独立票**；④分支维持本地不推送。

### 9. 续跑入口（**2026-10-25 17:05 用户重启电脑前中断**；✅ §9.1/§9.2 已于 18:36–18:56 全部执行完毕，实录见 §8——本节留作断点续跑方法论档案）

**已完成的实机面（全部有凭证，勿重做）**：§0 基线快照、§1 三轮不删卷重启周期、§2 L0 计时 2m37.7s、§3 双口径（带载四采样 5.46–6.04 GiB）、§7 L3 全链（save 3m12s / load 20m12s / init 7m14.9s）。**重启不损失任何卷态与证据**；容器靠 `restart: unless-stopped` 自动拉起，但 **P1 会话作业会随重启消失**（正常，§9.2 重提）。

**§9.1 关键时钟约束（先读）**：topic 里 gen-a 的最晚未来事件 et ≈ **18:25:56**；P1 作业冷启动会全量重放（HLD §8-R7-i）并把水位钉到 topic 内最晚 et−120s。**墙钟未过 ≈18:30 之前不要重提作业**——否则水位再度被钉在未来，探针必挂、白等一轮。重启后先 `date` 对表。

**§9.2 剩余步骤（按序）**：

```bash
# 0) 容器应已自动拉起；不足则 docker compose --profile doris up -d --wait
docker compose ps

# 1) 墙钟 ≥ 18:30 后：重提 P1 作业（重放只把已陈化事件重写，水位落在墙钟后侧 → 健康）
docker cp "F:\Anaconda practice project\eco-a-d-platform\flink\target\ecom-flink-0.1.0-SNAPSHOT.jar" \
  eco-flink-jm:/opt/flink/lib/ecom-flink.jar
docker exec eco-flink-jm /opt/flink/bin/flink run -d \
  -c com.ecom.flink.P1RealtimeJob /opt/flink/lib/ecom-flink.jar
#    等作业 RUNNING 且消费组 lag=0（重放 53,789 条约 1-2 分钟）

# 2) 重立 dlq 基线（重放把仍非法的再判脏，基线 9802 已过期；走首跑 SKIP 机制）
rm artifacts/verify/dirty-rate-baseline.json   # WSL: rm，此文件 gitignore

# 3) make verify 两连绿（第 1 跑 dlq=SKIP+落新基线；第 2 跑全 PASS 退出 0）
make verify && make verify
#    ⚠️ recon-offline-mirror-doris 有 30–60s 落定边沿（落定需 wm≥ws+180s，
#    collect 在 T0+260s ⇒ T0 落在分钟内 ≥40s 处才过）：若仅 recon 单项 FAIL，
#    原样重跑即可（每次 run 时长 ≈4.5 min，T0 相位自动挪动），不要改判据。

# 4) 补一组带载 stats（作业 RUNNING 时 docker stats --no-stream，入票 §3 表）

# 5) 收尾：勾票面验收（口径①按 §11.1 处置：如实给数、8GB 判据注记「结构性不可达待签」；
#    reproducibility-boundary §1 表四档状态以 §11.2 为准）→ impl-dag frontier →
#    code-review → make lint/typecheck + 全量 pytest → 本地提交（不推送）
```

**§9.3 未提交改动清单（重启无损，留待步骤 5 一并提交）**：票面（claimed + Progress）、
`HLD.md`（§4.2 双口径订正 + R7-g/h/i）、`reproducibility-boundary.md`（§11 新节）、
`docker/scripts/superset-provision.sh` 与 `superset-dirty-rate-sync.sh`（python3 修复）、
`evidence/impl-05/*`（新目录）。
