# impl-06f-4: 收尾门 — 部署面全判据 + `make init`→`verify` 一绿（或如实登记端口限制）

Type: task
Status: resolved
Blocked by: impl-06f-1, impl-06f-2

**What to build:** 用**一条真机命令链**证明这套东西现在处于「可交付」状态：
不设任何端口 override、跑完整部署面判据、再从 `make init` 走到 `make verify` 退 0。
如果端口这关仍然过不去，交付物就是**一份写得清清楚楚的已知限制**，而不是假绿。

**Blocked by:** impl-06f-1, impl-06f-2
（先有 README 与票面的落笔位置：判据条数与「已知限制」小节要先存在，
跑完发现端口不通时结论才有地方写。）

## 为什么单独成票

[impl-03a](../issues/impl-03a-p2-deployment-surface.md) §C 明确写了
「⚠️ **遗留项（明确不勾销）**：⑦『冻结端口』这半边**尚未**在真实端口上验证过。
端口恢复后须**不设** `COMPOSE_EXTRA_FILES` 重跑一次……必须在 impl-05 终验前补跑。」
impl-05 已收口，而这半条**至今没有补跑**——它是全仓唯一「已知没验、且被明确要求验」
的东西。

**2026-10-26 实测现状**：冻结端口只有 `8081` 通（`curl` 返回 200），
`8030 / 8040 / 8088 / 9030 / 9060 / 9092 / 9010` 全部连不上；`docker ps` 显示
全部端口已按 `127.0.0.1:<port>-><port>` 映射，且 `8081` 同时被 `eco-flink-jm` 占用
（`eco-flink-tm` 只有容器内 8081）。⇒ 不是 compose 写错，是宿主侧转发/防火墙。

## 执行序列（顺序不可换）

1. **前置**：确认栈已起、`make build-flink-image` 已就绪、作业 jar 在 `usrlib`。
2. **部署面全判据**：**不设** `COMPOSE_EXTRA_FILES`，跑 `bash scripts/deploy_surface_check.sh`。
   ⚠️ 副作用：条目 ③ 会 `docker compose down` 再 `up -d`（**不带 `-v`**），
   期间全部容器重启、**正在 RUNNING 的 Flink 作业会被杀掉**——这是票面要求的实证，
   跑前须先记录作业状态、跑后按需重提。
3. **端到端一绿**：`make init` → `make verify` 退出码 0。
4. **端口核实**：逐个 `curl 127.0.0.1:<冻结端口>`，把实测结果原样写进票面。

## Acceptance criteria

- [x] 不设 `COMPOSE_EXTRA_FILES` 跑 `deploy_surface_check.sh`，11 条判据**逐条打印原始输出**，结果原样入票
- [x] 若 11 条全 PASS：写明退出码 0，并**销掉** impl-03a §C 的「冻结端口」遗留项
  （勾选注记：本轮**非**全 PASS（10/11，退出码 1），条件句前半不适用；但后半
  「销掉遗留项」以**独立证据**达成——八端口全可达的实测 + 判据 ⑦ 本身 PASS，
  impl-03a §C 已落销项行。FAIL 分支的处置见第 3 条与 Answer 归因节。）
- [x] 若仍有 FAIL：逐条归因（判据自身缺陷 / 宿主环境 / 部署面真缺陷），**不得**用 override 掩盖后再宣称通过
- [x] `make init` → `make verify` 的实测结果（阶段耗时、PASS/FAIL/SKIP 计数、退出码）入票
- [x] 冻结端口逐个 `curl` 的实测表入票；端口不通时在 README「已知环境限制」与 impl-03a §C **同时**落笔
- [x] 跑完后把 Flink 作业恢复到 RUNNING（或如实记录"已停止及原因"），不留半死状态
- [x] 本票结论写进 `issues/impl-dag.md` 的 Frontier

## Notes

- **无法提权**：本机 WinNAT 排除 `8024–8123`，释放需管理员；本会话不请求提权。
  因此本票**允许以"已如实登记的限制"过关**——判据是「说法与实测一致」，
  不是「必须全绿」。
- **不要**为了让它绿而改判据、加 override、或放宽 11 条中的任何一条；
  判据口径的变更必须单独立票（先例：[impl-06e](impl-06e-deploy-check-usrlib.md)）。
- ③ 的重启副作用是**破坏性**的：跑之前先确认没有别的会话正在依赖 RUNNING 作业。

## Answer（2026-10-26 收尾门实跑）

**结论：收尾门以「10/11 判据 PASS + 1 条判据自身缺陷如实归因 + init→verify 一绿 + 八端口全可达」过关。**
`COMPOSE_EXTRA_FILES` 全程未设：01 环脚本内显式 `unset`（其输出首行即凭证），00 前置快照
（13:06）与 04 端口探测（13:32）各自回显 `COMPOSE_EXTRA_FILES=[]` 交叉印证；03 环 make 本身
不读该变量（`docker-compose.yml` 直接生效面在 ①–⑧ 判据内已验）。完整原始输出按环落盘
`.scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/`（00 前置快照 / 01 判据 / 02 作业恢复 /
03 init-verify / 04 端口 / 05 终态，各配同名 `.out.txt`）。

### 执行序列实录（UTC）

| 环 | 时间 | 结果 |
| --- | --- | --- |
| 0 前置 | 13:06 | 栈 7 服务在跑、usrlib 两端齐、host jar 在、墙钟领先 topic 最晚 et **23909 s**（§9.1 约束远超 120 s）、作业列表**空**（跑前无 RUNNING 作业可杀——③ 的破坏面如实记录）、`COMPOSE_EXTRA_FILES=[]`（快照第 2 行凭证） |
| 1 部署面 | 13:08–13:15（437 s） | **10 PASS / 1 FAIL，退出码 1**（详下） |
| 2 作业恢复 | 13:21–13:22（56 s） | JobID `16ddd10b…` RUNNING（36 tasks）、消费组 **5 s 即 lag=0**（续读 committed offsets，DLQ 不翻倍）、usrlib 重拷两端同字节 |
| 3 init→verify | 13:23–13:32（531 s） | `make init` **127 s 退 0**（四段 + `doris-assert` 全 PASS）；`make verify` **404 s，PASS=10 FAIL=0 SKIP=0 ⇒ 退出码 0**，报告 `verify-report-20261026T133211Z.json` |
| 4 端口 | 13:32 | **八个冻结端口全可达**（HTTP 面 4×200 + 协议面 4×TCP OPEN + docker port 映射逐个核对） |
| 5 终态 | 13:33 | 主流作业 RUNNING（lag=0）、全栈 7 服务 healthy、usrlib 两端齐 |

### 判据逐条（①②③④ 4b 4c 4d ⑤⑥⑦⑧，共 11 条）

① PASS（compose 层两服务各恰 1 条 `paimon-warehouse→/warehouse`；实机 inspect 两侧卷 Name
逐字相等 `eco-a-d-platform_paimon-warehouse`）｜② PASS（JM 写探针→TM 读回逐字相同，含本轮
stamp+nonce）｜③ PASS（down **不带 -v**→up 后探针逐字仍在；安全自检无 `-v` 拦截）｜④ PASS
（两端 usrlib `ecom-flink.jar` size=73693749 + sha256 逐字相同，**恰 1 个** ecom jar；lib 零
副本）｜4b PASS（两端 uid=9999 一致 + 实写读回逐字相同）｜4c PASS（两端 `flink --version` =
2.2.1，ADR-0002 锚点成立）｜4d PASS（两端 `paimon-flink-2.2-2.1.0.jar` size/sha256 与钉死值
逐字一致）｜**⑤ FAIL（唯一失败，归因见下）**｜⑥ PASS（`git check-ignore -v` 对 `data/` 与
`data/x` 双命中 `.gitignore:10 **/data/`）｜⑦ PASS（常驻 6 服务全 healthy + kafka-init
exited 0 + 服务条目数=7 排除 client）｜⑧ PASS（compose 5 处 / docker/flink 9 处 / Dockerfile
9 行 / flink 段 2 行标注词全在）。

### ⑤ 的逐条归因（判据自身缺陷——三分类里的第一类）

命中 32 行**全部不是可执行 SQL**：27 行来自 `.mimosa/hook-state/**`（DSH harness 会话状态
目录——判据写就时不存在的未跟踪目录，里面是本仓文件的 baseline 副本**含判据脚本自身**）；
4 行来自 `tests/test_replay_sql.py` 的 docstring/断言消息（它守卫的正是这条判据，自己却被
命中）；1 行 `README.md:64`（判据名列举，impl-06f-1 新增）。判据的注释排除法（`#` 开头行）
覆盖不了 Markdown 叙述 / Python docstring / harness 状态目录三种形态 ⇒ **恒假方向缺陷**。
按 Notes「不得改判据、口径变更必须单独立票」处置：**已立 [impl-06i](impl-06i-deploy-check-5-exclusion-defect.md)**
（三修法方向 A/B/C 供裁决 + 反例凭证要求），本票不动判据一支。

### impl-03a §C「冻结端口」遗留项：**销项**

跑时排除段已漂走（impl-06f-1 晚间复核口径延续）：八个冻结端口 HTTP 面
`8030/8040/8081/8088` 全 **200**（body 各为 Doris FE health JSON / BE metrics / Flink
overview JSON / Superset `OK`），协议面 `9010/9030/9060/9092` 全 **TCP OPEN**（curl 对照组
如预期非 200——协议层失败 ≠ 端口不通），`docker port` 映射逐个核对与 compose 声明一致。
**「⑦『冻结端口』这半边尚未在真实端口上验证过」至此有了不设 override 的一手实证。**

### make verify 十项细目（退 0）

kafka-topic-retention PASS（两键与 gdd §A.2 全等）｜flink-job-running PASS（RUNNING）｜
paimon-five-layers PASS（ods=3145728 / dwd=1048576 / dim=105430 / dws=1550285 / ads=90348）｜
doris-ads-rows PASS（31113/2556/36293 逐表 >0）｜superset-health PASS（OK）｜
funnel-monotone PASS（[734,704,613,505] 非增）｜dlq-rate PASS（0.271960 ≤ 基线 0.272138；
DLQ 14911 **不变**——06d 案 2 续读位点使冷启重放不翻倍）｜is-settled PASS（pending=3033 /
settled=28080 两态都在）｜recon-probe PASS（rows=3 逐值全等）｜degradation-ladder PASS
（oom_killed=0）。

### 附带收获（如实记录）

- ③ down/up 后 usrlib **整个目录消失**（可写层不随 down 保留）⇒ 作业恢复必须重拷 jar 再提
  交——这是「作业 jar 走 docker cp 而非卷/镜像」路线的固有运维事实，已随 02 环实录。
- verify 期间 sql-client 批探针作业自然 FINISHED（5 个，2–12 tasks），非半死状态。
- 静态面：lint PASS / typecheck PASS（0 errors） / pytest **314 passed**——凭证
  `evidence/impl-06f-4/06-static-face.out.txt`（首跑 52.49 s，归档复跑 75.52 s，两次均 314 passed）。
- `make init` 第三段 `superset-provision.sh` 每次**刷新** `docker/superset/export/ecom-live.zip`
  （impl-04 固化的「导出→回读」闭环）。实测新旧 zip 解包 diff：看板 payload 逐字相同，
  差异仅导出目录名与 `metadata.yaml` 的 `timestamp` 两处时间戳。已将 zip 还原为 HEAD 版本
  （提交面保持纯文档）；此现象非缺陷，仅为 init 的固有刷新行为记录。
