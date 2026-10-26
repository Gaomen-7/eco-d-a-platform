# impl-06e: 部署面判据 ④ 改口径——作业 jar 必须验在 /opt/flink/usrlib/

Type: task
Status: resolved
Blocked by: —（impl-06d 已结案；本票只改判据面，不动运行面代码）

**What to build:** 把 `scripts/deploy_surface_check.sh` 的条目 ④ 从
「Paimon jar 在 JM+TM 两容器 `/opt/flink/lib` + size/sha256」改为
「作业 jar 在 `/opt/flink/usrlib/`」，因为 impl-06d 实测**作业 jar 放 `lib/` 会
`LinkageError: loader constraint violation … committedOffsets`**，而原判据
**结构上不查作业 jar 所在目录**（放错目录时它照样 PASS）。

**验收标准**

- [x] ④ 改判 `/opt/flink/usrlib/`：两端各一份 ecom 作业 jar，**逐字节相同**（size+sha256）
      —— 只给一端拷过（半部署）必须被抓到
- [x] ④ 增设**阴性**判据：两端 `/opt/flink/lib` 不许有作业 jar 副本（LinkageError 形态）
- [x] 原「Paimon connector 在两端 lib + size/sha256」断言语义**不丢**，下移为 4d
- [x] 反例证伪先行：三个反例各自 FAIL、还原后 PASS，且**每轮各只计 1 条 FAIL**（无重复计数）
- [x] 判据从 10 条变 11 条（①②③④ 4b 4c 4d ⑤⑥⑦⑧），脚本头注与 main() 自述同步改准
- [x] impl-03a 票面登记口径变更（§F），避免下个会话照旧口径复核

## Notes

- **改的是已验收票（impl-03a）的判据面**：只改**判据对象**，不改 ③ 的 down/up 与
  ⑤⑥⑧ 的语义；那些条目本轮**未复跑**，故不声称全绿。
- **本条不新增运行面约束**：`/opt/flink/usrlib/` 是 impl-06d 已登记的环境事实
  （impl-02d §9 环境级坑 + .uv-cache/deploy-job.sh 一直在做的事）。
  本票只是把它变成**可机读判据**。
- 判据条数仍以 `main()` 的调用清单为准，不以任何票面/本节写的数字为准。

## Answer（2026-10-26）

**结论：④ 已改判，正路径 PASS + 三个反例全部被抓到（注入后 FAIL、还原后 PASS）。**

### 判据形态（现行）

| 段 | 判什么 | 抓什么反例 |
| --- | --- | --- |
| (a) 阳性 | 两端 `/opt/flink/usrlib/` 各 1 个 `ecom*.jar`（`.jar$` 锚定），size+sha256 **逐字相同** | 只给一端拷过；usrlib 多版本共存；jar 为 0 字节 |
| (b) 阴性 | 两端 `/opt/flink/lib/` **零** ecom jar | usrlib 有、lib 也有 ⇒ LinkageError 形态 |

### 实机读数（2026-10-26，本轮）

```
[eco-flink-jm] usrlib: -rwxrwxrwx ubuntu ubuntu 73693749 ecom-flink.jar
[eco-flink-jm] size=73693749 sha256=9031125acacdfe0f76c9fd53d00c6ecfcbe91169b92fe899f8831ebbf2b87324
[eco-flink-tm] size=73693749 sha256=9031125acacdfe0f76c9fd53d00c6ecfcbe91169b92fe899f8831ebbf2b87324
=> 两端逐字节相同；lib/ 两侧零 ecom
[PASS] ④ 作业 jar 在 usrlib 且 lib 无副本
[PASS] 4d Paimon connector 在两端 lib   （57697871 / 43a6416a…107dd1，钉死值未变）
```

### 反例证伪（一次性注入，已全部还原并复验）

| 反例 | 注入动作 | 结果 |
| --- | --- | --- |
| A | `cp usrlib/ecom-flink.jar lib/`（JM） | `[FAIL] … /opt/flink/lib/ 里有作业 jar 副本 ⇒ 父子类加载器双份 ⇒ LinkageError` |
| B | `mv usrlib/ecom-flink.jar …jar.off`（TM） | `[FAIL] eco-flink-tm 侧 /opt/flink/usrlib/ 里没有 ecom 作业 jar` |
| C | `touch usrlib/ecom-old.jar`（JM） | `[FAIL] … usrlib 里有 2 个 ecom jar，期望恰 1` |

三轮各**1 条** FAIL（不是 2 条）；每轮还原后立即回 `PASS`。
还原核对：两端 usrlib 各 1 个 `ecom-flink.jar`（73693749 B）、lib 零 ecom 副本、作业 `cdf679cd…` 仍 RUNNING。

### 反例证伪踩出的两个自身缺陷（已修，别再踩）

1. `ecom.*\.jar` 未锚定行尾 ⇒ B 的 `ecom-flink.jar.off` 被当成 jar，**B 反而 PASS**（假绿）。
   修：改用 `^-.*ecom.*\.jar$`（`ls -l` 的 `$NF` 即文件名）。
2. 「两端比对」与「单侧缺失」各记一次 ⇒ C 一轮计 **2 条** FAIL（impl-03a §D #2 计数重复缺陷回潮）。
   修：单侧失败置 `side_bad=1`，比对段跳过并打印跳过原因。

### 未做

- ③（down/up）未跑：会重启整栈、杀掉 RUNNING 作业（36 tasks），且不属于本条范围。
- ⑤⑥⑧ 未跑（⑤ 的全仓逐文件 `grep -r` 在本机 MSYS 上超时）。三条语义均未改。
- ①⑦ 在本机复跑同样 FAIL，但用 `git show HEAD` 的**改动前脚本**跑出**同样两条 FAIL**
  ⇒ 属宿主环境既有问题（`docker compose config/ps` 在该 shell 里输出为空），**与本改动无关**。
