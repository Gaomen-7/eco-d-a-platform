# impl-03f: Paimon↔Doris 对账 + `make verify` 第三项由 SKIP 转真断言

> 父票：impl-03
> 规格落点：spec §5.2（ADS 与 Doris 键同构）、§5.4（对账只比 `is_settled=1`）、impl-03 母票 ①②⑤

Type: task
Status: ready-for-agent
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

- [ ] Paimon 与 Doris 两套 `ads_window_metric` 在 `is_settled=1` 行上**逐值严格相等**（逐 `metric_name` 列出实测值，留下输出）
- [ ] 人为改动 Doris 侧**一个 `is_settled=1` 的落定值**后，对账断言**失败并退非 0**
- [ ] 对账**只比 `is_settled=1`**：`is_settled=0`（暂定）行的差异**不得**导致对账失败
- [ ] `make verify` 第三项 `paimon-five-layers` 由 `SKIP` 转为**真断言**，实测 `PASS`/`FAIL` 并计入退出码
- [ ] `make verify` **五项全真**、退出码 0，且 **两连续绿**（证明不是偶然一次）
- [ ] impl-02e 已验收的 P1 判据（八项）在 `verify` 转真后**仍然通过**（没有被 P2 改动带坏）
- [ ] impl-03 母票 10 条验收**逐条核对勾选**，每条附实测证据
- [ ] 门槛批文件落 `data/` 且被 `.gitignore` 覆盖；`git check-ignore` 验证（仓库内无原始数据）
- [ ] 官方未给出的按 spec §8.3 **登记**（`sys.compact` 对主键表的逐句适用声明、`file://` compaction 成本数字、Flink SQL 断言失败→非零退出码机制＝**源码显示不存在**）—— **遇到就登记，不臆造**
- [ ] 更新 impl-dag：impl-03 结清、Frontier 推进

## Notes

- **本票是唯一改 `verify_runner.py` 的 P2 票**。前五张副票都不得动第三项，避免 P2 途中把 P1 已验收的判据改坏。
- 「五层各 `COUNT(*)>0`」走 **Flink SQL**，**不是** Doris 的 `total_record_count`（`sql/README` 与 impl-02e 票面均留了这条约束：快照数判据须走 Flink SQL，Doris 不在 Paimon 系统表支持列表内）。
- `make replay` 幂等**不进** `verify`（spec §8.2 已定）：全量重放两遍是重操作，由 03d 的离线幂等断言 ＋ impl-02 的 `verify-fault` 覆盖。
- **不实现**（spec §8.2）：两组 ADS 各自供看板 · 「离线重算覆盖实时行」（记 COULD 留档，P2 判据不含它）· 幂等进每次 `verify`。
- 本票完成后 **impl-03 结清**，impl-05（终验）的双闸门之一随之打开（另一扇门是 impl-04 看板）。
- 执行面：宿主无 `make`，须在 WSL 下跑；`docker` 命令需 `MSYS_NO_PATHCONV=1`。`.env` 含 `SUPERSET_SECRET_KEY`，属机密面，不要 grep 或读取。
- 不跑 `down -v`；L0 零状态实测归 impl-05。