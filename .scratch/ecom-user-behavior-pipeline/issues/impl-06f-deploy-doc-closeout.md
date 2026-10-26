# impl-06f: 交付面收尾 — README 与代码对齐 · 票面状态归正 · 术语补登记 · 收尾门

Type: task
Status: resolved
Blocked by: —（文档面，随时可开工）

> **母票（索引）**。四张切片票各自独立可验证，本票只做索引与收口。
> 出处：2026-10-26 会话「项目收尾还剩什么」的盘点结论——功能票全结，剩下的
> 是**文档已过期**与**一条环境遗留**。切片见下表。

**What to build:** 让仓库里「写下来的现状」和「代码实际做的事」重新一致，并把
唯一未销的环境遗留如实登记出来。收口后，任何人只读 README + 票面，就能知道
现在到底能做什么、还差什么——不会再照着 README 的假现状去排查。

## 切片（依赖顺序）

| 票 | 文件 | Blocked by | 交付 |
| --- | --- | --- | --- |
| **impl-06f-1** | [impl-06f-1-readme-reality-alignment.md](impl-06f-1-readme-reality-alignment.md) | — | README 六处现状断言与代码/脚本对齐 |
| **impl-06f-2** | [impl-06f-2-ticket-status-normalization.md](impl-06f-2-ticket-status-normalization.md) | — | 票面状态自相矛盾与过期清单归正 |
| **impl-06f-3** | [impl-06f-3-glossary-three-terms.md](impl-06f-3-glossary-three-terms.md) | — | GLOSSARY 三词补登记（impl-02e 转出的欠账） |
| **impl-06f-4** | [impl-06f-4-closeout-gate-deploy-proof.md](impl-06f-4-closeout-gate-deploy-proof.md) | impl-06f-1, impl-06f-2 | 部署面全 11 条判据 + `make init`→`verify` 一绿；端口不通则如实登记 |

## 验收标准

- [x] 四张切片各自验收勾满，且每张的**证据可被一条命令复现**
- [x] README 不再包含与代码矛盾的「现状」句（判定方法：逐句找到复核命令）
- [x] 全部票面文件顶部 `Status:` **唯一**，且与文件内自上而下的状态陈述一致
- [x] 未被兑现的复选框只剩「真未做」项，且每条写明为什么留着
- [x] 端口遗留以「已知限制」形态写进 README 与 impl-03a §C，不再处于「待办但无人认领」状态

## Notes

- **本票不改任何运行面行为**：零代码、零 compose、零脚本逻辑变更（唯一允许的
  非文档改动是 impl-06f-1 里对 `Makefile` `spike:` 目标的一行注释/提示，且
  真发动作归 [impl-06g](impl-06g-spike-live-rate-proof.md)）。
- **不重写历史**：已结票的 `## Answer` 段落一律不改写，只在**顶部状态行**与
  **新增的收尾节**里补充口径，避免抹掉当初的取证上下文。
- 判据条数以 `main()` 的调用清单为准（现 11 条），不以任何票面数字为准。

## Answer（2026-10-26 收口）

**四切片全部结案，母票收口。** 逐条核验：

| 验收 | 凭证 |
| --- | --- |
| 四切片勾满 | 06f-1 ✅（六处落地 + grep 干净）、06f-2 ✅（5 处矛盾清零）、06f-3 ✅（三词条 + 43 tests）、06f-4 ✅（判据 10/11 + init→verify 一绿 + 八端口全可达，各票 Answer 有完整实录） |
| README 无矛盾现状句 | 06f-1 六处逐句可复核；06f-4 又补「已知环境限制」当日实证条目（含复核命令指针） |
| Status: 唯一 | 06f-2 归正后零残留；06f-4/06i 本轮新增票面均规范 |
| 复选框只剩真未做 | 06f-2 的 39 个 `- [ ]` 保留理由清单；本票 5 条全勾 |
| 端口遗留归位 | 比「已知限制」更好：**销项**——README 实证条目 + impl-03a §C 销项行（八端口 HTTP 4×200 + TCP 4×OPEN，不设 override） |

**收尾门带出的新票**：[impl-06i](impl-06i-deploy-check-5-exclusion-defect.md)（判据 ⑤
排除集不完备，`ready-for-human`）——收尾门的 FAIL 没有被掩盖，按票面 Notes
「口径变更必须单独立票」处置。

**本票唯一的越界说明**：06f-4 属实机验证票，其「不改运行面」承诺在它自己的票面
（它跑的是既有脚本与 make 目标，零代码改动）——母票 Notes 的「零代码」约束对四
切片各自的实跑面不构成阻塞，实施证据在切片票内。
