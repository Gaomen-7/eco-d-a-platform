# 03: 仓库收尾与全量验收（S1–S4 闸门）

**What to build:** 骨架的最后一段垂面：README（≤60 行——一句话定位、前置 daemon/WSL2、`make init` 一条命令四段冻结、八冻结目标表、设计链指针、本地提交不推送纪律，修 D-I①）＋ `sql/` 占位（一行指针至地图 D9 五层形状表，DDL 归 P2）＋ gitignore 增运行产物目录（verify 报告落点）。然后 **S1–S4 全量重跑**并贴实机输出：`make lint`/`make typecheck`/`make config-check` 全 exit 0、uv 三连（sync/`lock --check`/import 冒烟）、`mvn -q package` BUILD SUCCESS、`docker compose config -q` 通过（免 daemon）。全绿的承诺在这张票兑现——它是 integrate-and-verify 切片。

**Blocked by:** 01, 02.

**Status:** ready-for-agent

- [ ] README ≤60 行且含八冻结目标表与 `make init` 四段说明
- [ ] `sql/` 占位指针在位
- [ ] gitignore 含运行产物目录
- [ ] S1–S4 全量重跑，实机输出贴进交付回复（沿 P0「stdout 行＋退出码」形态）
- [ ] 收尾本地 commit（`feat(skeleton)` 风格）、未推送；`git status` 干净
