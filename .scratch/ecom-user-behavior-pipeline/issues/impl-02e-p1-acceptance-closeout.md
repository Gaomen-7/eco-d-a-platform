# impl-02e: P1 收口验收 —— make init/verify 全绿 + 勾票 + code-review + commit

> GitHub: https://github.com/Gaomen-7/eco-d-a-platform/issues/5
> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: ready-for-agent
Blocked by: impl-02b, impl-02c, impl-02d

**What to build:** 在干净的已有制品机器上跑通 `make init` → `make verify` 全绿退出 0，把 impl-02 的 10 条验收逐条勾上并本地提交。

## 背景

代码侧已完成并已本地提交（commit `c9cf0cc`，pytest 88 passed）：四阻断已解、`is_settled` 两态实机可见（12/242）、漏斗 `revision` 1/2 实机可见、重复计数真口径已改。剩余全是**收口验证**，不需要新的设计判断。

这是 P1 阶段闸门票：spec §4.3 明写 P2（impl-03）只在 P1 四项通过后才开，impl-04 看板也以本票为前置。

## Acceptance criteria

- [ ] `make init` 在**已有制品**的机器上跑完后，`make verify` 退出码 0、`docker compose ps` 全 `healthy`，且 `make init` 输出里没有「待补」字样
- [ ] `make verify` 打印五项逐条 `PASS/FAIL/SKIP <名> <实测> vs <期望>`，**第三项显式 SKIP**（Paimon 五层，P1 未开离线分层），且退出码仍为 0
- [ ] 一次运行中可观察到「窗口关闭（暂定→落定）与迟到修正时刻 `revision`+1」
- [ ] JSON 报告逐断言落状态；**人为制造一次 FAIL 时**，同目录出现原始响应快照全文
- [ ] 票面 10 条验收逐条核对勾选，每条附实测证据（命令 + 输出）
- [ ] 跑 `code-review`（改动跨 compose / Java / Python 三栈）并处理结论
- [ ] 本地 commit（**绝不 push**；本仓硬纪律）
- [ ] 更新 impl-dag 的 Frontier 指针，标明 impl-02 结清、impl-03/impl-04 可开

## Notes

- **不跑 `down -v`**：会销毁 P0 已验证的卷态成果；L0 零状态实测归 impl-05。
- 不重跑 P0 破坏性实测。
- 执行面：宿主无 `make`，须在 WSL 下跑；`docker` 命令需 `MSYS_NO_PATHCONV=1`。
- `.env` 含 `SUPERSET_SECRET_KEY`，属机密面，不要 grep 或读取。
- 复测 Stream Load 读 `/opt/flink/log/*.log`，不要用 `docker logs`。
- 判据的「反例 ⇒ FAIL」证据散在 impl-02c/02d，本票只需汇总引用，不必重造。