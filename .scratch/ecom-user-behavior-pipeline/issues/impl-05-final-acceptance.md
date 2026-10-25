# impl-05: 终验 — 容量双口径 · 不删卷重启 · 冷启契约 · 附录 A 结清

Type: task
Status: ready-for-agent
Blocked by: impl-03, impl-04

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

**Status:** ready-for-agent

## Acceptance criteria

- [ ] 容量两个口径各自给出数字：声明上限之和 ≤ 8 GB、`docker stats` 带载实测合计 ≤ 8 GB，且**实测是 P1 带载状态下测的**（不是空载）
- [ ] 三个状态进卷的组件各完成一次不删卷的重启周期，重启后数据仍在；Doris FE 无 `feType:UNKNOWN` 卡死
- [ ] L0 从 `make init` 到全 healthy 的实测耗时 ≤ 15 分钟（记录起止时刻），且明确标注「预拉镜像前提」
- [ ] L3 用 `docker save`/`docker load` 路径实测冷启 ≤ 30 分钟，**构建 Doris 镜像的时间不计入**
- [ ] 附录 A 的 M/N/O 三条已在上游文档生效；本轮新增的偏离行已登记进 HLD §8-R7 或 `reproducibility-boundary.md`
- [x] `minio`/`spark` profile 拿到用户裁决（实现 or 撤需求），撤需求时 SRS 已按意图类改写并留签字记录
      ★ 2026-10-25 用户裁决**撤需求**：SRS `REQ-F-ORCH-08` 已改写（spec §7.1 #40 签字记录）；
      spec §8.1 / `map.md` Out of scope / gdd §1.11 同步订正；spec §7.5 #36 标已结清。
- [ ] 所有数字标注 `官方给出` 或 `设计值`，**没有把设计值当官方保证**
- [ ] 全部本地提交，未推送

## Notes

- **须先获批的实机 task**（spec §8.2，**不可自开**）：HOUR 分级保留演练实机 · DROP 后回盘实测 · compaction ×20 GB 对撞。三张都涉及对已验证卷态的破坏性操作，需先向用户说明风险并取得明确同意。
- **L0 零状态实测（`down -v`）不在本票**：需销毁 P0 已验证的卷态成果；本票只做不删卷的重启周期。L0/L1/L3 在证据不足时标「待测」而不是编数字。
- **回收面**：本票完成后 spec 的九节全部有施工落点；`map.md` 不回开，实现进度由本组票承载。