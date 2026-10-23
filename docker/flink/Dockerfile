# Flink 2.2 镜像（本地构建）—— 在官方原版镜像上**烘焙** Paimon bundled connector
#
# 构建：
#   docker build -f docker/flink/Dockerfile -t eco/flink:2.2-paimon docker/flink
# 覆盖镜像（与 docker/doris 同款先例：\${DORIS_IMAGE:-eco/doris:4.1.5}）：
#   \${FLINK_IMAGE:-eco/flink:2.2-paimon}
# 回滚：docker rmi eco/flink:2.2-paimon；jar 由 scripts/fetch-paimon-jar.sh 重新获取。
#
# ★★ 为什么必须在**构建期**把 jar 放进 /opt/flink/lib（本票的核心决定）★★
#
#   (1) **官方逐字要求装进「每一个」Flink 进程**。Paimon 官方文档（Flink 快速上手 /
#       Deployment 一节）原文：
#         "Install one matching Paimon bundled connector in **each** Flink process."
#       本项目的 flink-jm 与 flink-tm 是**两个独立容器**（compose 里两个 service），
#       各自的 /opt/flink/... 互不互通 ⇒ 只往一边放，另一边的 classloader 看不到
#       Paimon 的 Factory，SQL 里 `CREATE CATALOG ... 'connector' = 'paimon'` 直接
#       "Could not find any factory for identifier 'paimon'"。
#       两个 service 用**同一个镜像** ⇒ 一次烘焙即满足「each」。
#
#   (2) **`ADD JAR` 指向宿主路径不可行**（票面已定案的否决项）：
#       `ADD JAR` 是 Flink SQL **会话内**命令，路径按**容器内**文件系统解析；
#       jar 只存在于宿主（或 WSL）的 docker/flink/lib/ ⇒ 容器内该路径不存在。
#       把宿主目录 bind-mount 进两个容器再 ADD JAR，等于把「运行时状态」耦合到
#       「宿主目录布局」，且 JM/TM 都要各自 mount ⇒ 违背「构建期固化」的目标。
#       ⇒ 唯一稳的形态：构建期 COPY 进镜像自带的 /opt/flink/lib/（Flink 启动时
#       自动把它加进 classpath，见官方 flink:*-java17 镜像的 ENV FLINK_HOME 约定）。
#
#   (3) **为什么 jar 预取到构建上下文，而不是在构建期 curl 下载**：
#       官方 Docker 最佳实践要求构建**可复现**（"reproducible"）；构建期联网下载
#       会把镜像内容绑定到「构建那一刻镜像站的可用性与内容」上——同一份 Dockerfile
#       在不同时间/不同网络可能得到不同字节，且**离线重跑必然失败**。
#       更关键的是：本仓库的 Doris 先例（docker/doris/Dockerfile）之所以在构建期下载，
#       是因为 4.32 GB 的 tarball 经构建上下文传输实测 3276 秒后 EOF；本例 jar 只有
#       57 MB（设计值：57,697,871 B，实测），上下文传输代价完全可接受，
#       因此**没有必要**引入构建期联网这条不可复现路径。
#       ⇒ 预取 + sha256 钉死 = 构建期只做 COPY，可离线重跑，字节级可审计。
#       预取入口：scripts/fetch-paimon-jar.sh（幂等，自带 size+sha256 校验）。
#
# ★ jar 身份（审计用，来自 Maven Central 官方坐标）：
#   file   : paimon-flink-2.2-2.1.0.jar
#   size   : 57,697,871 B（实测，2026-10-22；设计值口径 = 本仓钉死的期望值）
#   sha256 : 43a6416a241ab736fb6ed2b6061a37fc5cca5ba8737d77eac1b977f859107dd1（实测）
#   source : https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-2.2/2.1.0/paimon-flink-2.2-2.1.0.jar
#   ⚠️ Paimon 官方**未给出**该 jar 的官方校验值；上面的 size/sha256 是本仓**实测钉死**的
#      期望值（"设计值"口径），不是官方保证。jar 与 Flink 的版本匹配关系（paimon-flink-2.2
#      ↔ Flink 2.2）来自官方 artifactId 命名约定，非本仓臆造。

FROM ${REGISTRY_MIRROR:-docker.m.daocloud.io}/apache/flink:2.2-java17

# 审计标签：把 jar 身份写进镜像元数据，日后 `docker inspect` 即可核对，不必回读 Dockerfile。
LABEL org.opencontainers.image.title="eco/flink" \
      org.opencontainers.image.description="Apache Flink 2.2 (java17) + Paimon bundled connector paimon-flink-2.2-2.1.0" \
      eco.paimon.jar="paimon-flink-2.2-2.1.0.jar" \
      eco.paimon.jar.sha256="43a6416a241ab736fb6ed2b6061a37fc5cca5ba8737d77eac1b977f859107dd1" \
      eco.paimon.jar.size="57697871" \
      eco.paimon.jar.source="https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-2.2/2.1.0/paimon-flink-2.2-2.1.0.jar" \
      eco.paimon.hadoop_uber.jar="paimon-hadoop-uber-2.1.0.jar" \
      eco.paimon.hadoop_uber.jar.sha256="54290a429afd0a5c106ea285dc4c5c1bc2376d11f911b149d143e5ecca1fa004" \
      eco.paimon.hadoop_uber.jar.size="50615937" \
      eco.paimon.hadoop_uber.jar.reason="apache/paimon#6654 (Open): file: warehouse still needs hadoop Configuration classes; supplied via official paimon-hadoop-uber 2.1.0"

# ★ 构建期 COPY（不是 ADD JAR，不是 mount，不是 RUN curl）。
#   上下文 = docker/flink/（见构建命令），故源路径是 lib/... 而非 docker/flink/lib/...
#   目标 /opt/flink/lib 是**官方 flink 镜像自带的** classpath 目录（Flink 启动脚本会
#   把该目录下所有 jar 加进进程 classpath），因此 JM 与 TM 用同一镜像即各自加载。
#
#   ★ RUN 内做一次**构建期自检**（sha256 + 非零字节）：
#     目的不是替代 fetch 脚本的校验（那是下载侧），而是把「上下文里的 jar 被替换/
#     截断」这种事故挡在镜像外——COPY 是静默的，坏 jar 会一路进镜像直到 SQL 报错
#     才暴露。用官方 flink:2.2-java17 基础镜像自带的 sha256sum（coreutils，
#     官方镜像基于 eclipse-temurin/debian，实测存在）即可，不额外装包。
#     ⚠️ 若某天基础镜像精简掉 coreutils，此处会以 "sha256sum: not found" 失败——
#        这是**故意的**失败（宁可构建失败，也不要带未校验 jar 的镜像）。
COPY lib/paimon-flink-2.2-2.1.0.jar /opt/flink/lib/
RUN set -eux; \
    jar=/opt/flink/lib/paimon-flink-2.2-2.1.0.jar; \
    test -s "$jar"; \
    echo "43a6416a241ab736fb6ed2b6061a37fc5cca5ba8737d77eac1b977f859107dd1  $jar" | sha256sum -c -; \
    ls -l "$jar"

# ★★ paimon-hadoop-uber（**impl-03b 新增**，2026-10-22）★★
#
# 症状（impl-03b 实机）：Paimon 的元数据面全通，但**实际读写表**时炸：
#   INSERT INTO ... → java.lang.ClassNotFoundException: org.apache.hadoop.conf.Configuration
#   SELECT count(*) FROM <paimon表> → 同上
#   而 CREATE CATALOG / CREATE TABLE / SHOW DATABASES / SHOW TABLES **全部成功**。
#
# 为什么 bundled connector 自己不够：paimon-flink-2.2-2.1.0.jar 里**零** org/apache/hadoop/**
# （解包 27 319 entries 实测），只有它自己的包装类 org/apache/paimon/fs/hadoop/HadoopFileIO。
# 而 Paimon 在 file: warehouse 上仍会解析 hadoop 的 Configuration 类。
#
# ★ 这是**上游缺陷，不是官方要求**（取证见 .scratch/.../impl-03b §C）：
#   - 官方 Filesystems 表把「ensure that the cluster is in the hadoop environment」
#     **只写给 hdfs:// 那一行**；file:// 一行写的是 "Built-in Support"。
#   - apache/paimon#6654（**Open**，2026-10-22 读取时仍未关）标题逐字
#     "[common] CatalogContext requires Hadoop dependency even in non-Hadoop environments"，
#     正文称其 "causing NoClassDefFoundError in environments where Hadoop is not needed"。
#   ⇒ 所以这是 Paimon 的 hadoop 未解耦缺陷，补给方式正是官方自己写的用法（见下一段）。
#
# ★ 为什么是这个 jar：官方文档逐字
#   "If you want to read from and write to Paimon tables on a Hadoop cluster within an
#    application that has no Hadoop dependencies, you can use the Paimon Hadoop uber jar."
#   即「应用侧没有 hadoop 依赖」正是本仓的形态，官方为此提供了 uber 包。
#
# ★ 版本必须 **2.1.0**（与 paimon-flink 同版本，不是近版本）。
# ★ 类冲突已实测（2026-10-22），不是推断：两 jar 的 .class 重叠**仅 1 个**
#   （META-INF/versions/9/module-info.class，非业务类）；org/apache/paimon/fs、/catalog
#   关键类**零重叠** —— uber 的 hadoop 类走 paimon/fs/shade/hadoop3/ 影子路径，
#   故可安全共存于同一 /opt/flink/lib。
#
# ★ 票面口径的调整（已获用户 2026-10-22 裁决「选 A」）：
#   票面第 8 条「全 SQL、零 jar」原意是**业务侧零 jar**（不新增业务连接器、无需 JDK/Maven）；
#   本 jar 属**环境/部署面依赖**，归 impl-03a 的部署约束管理，不是业务 jar。
#   ⇒ 仍满足「ADD JAR 一条也没有」「不重读 Kafka」「不需要 JDK/Maven」。
#
# 期望值（本仓钉死的**设计值**，非官方保证；官方未公布校验值）：
#   size   = 50615937 B  （Central 页面 Content-Length，实测一致）
#   sha256 = 54290a429afd0a5c106ea285dc4c5c1bc2376d11f911b149d143e5ecca1fa004（本仓实测）
COPY lib/paimon-hadoop-uber-2.1.0.jar /opt/flink/lib/
RUN set -eux; \
    jar=/opt/flink/lib/paimon-hadoop-uber-2.1.0.jar; \
    test -s "$jar"; \
    echo "54290a429afd0a5c106ea285dc4c5c1bc2376d11f911b149d143e5ecca1fa004  $jar" | sha256sum -c -; \
    ls -l "$jar"

# ★★ 预建 /warehouse 并交给 flink 用户（impl-03a 实机取证后补，票面未写但**承重**）★★
#
# 症状（2026-10-22 实机，首次跑通挂载即暴露）：
#   docker exec eco-flink-jm sh -c 'echo x > /warehouse/probe'
#     → sh: 1: cannot create /warehouse/.probe-...: **Permission denied**
#
# 根因（两个条件叠加，缺一不成立）：
#   (1) **镜像以非 root 用户运行**：官方 flink:2.2-java17 的 Dockerfile 末尾是
#       USER flink（实测容器内 `id` → uid=9999(flink) gid=9999(flink)）。
#   (2) **镜像内原本不存在 /warehouse**（实测：`docker run --rm --entrypoint sh
#       eco/flink:2.2-paimon -c 'ls -ld /warehouse'` → `No such file or directory`）。
#       基础的 apache/flink 镜像**没有**这个目录，是本项目在 compose 里新挂的挂载点。
#
#   于是 Docker 在**创建该容器**时，因目标路径在镜像里缺失，只能**以 root:root
#   新建挂载点目录**（实测 `ls -ldn /warehouse` → `drwxr-xr-x 2 0 0`），
#   首次创建具名卷时又把这份 root 属主**留在卷根**。⇒ flink 用户对该卷只有 r-x、
#   没有 w。Paimon 要在该目录下建 database/表目录并写 bucket 文件 ⇒ 必失败。
#
#   ⚠️ 上一版注释把这里写成「具名卷首次挂载总会以 root:root 建目录」，**不准确**——
#      准确的说法是「镜像里没有该路径时，Docker 只能以 root 新建挂载点」。这条区分
#      正是下面第 ① 条修法能成立的前提，故必须写准。
#
# 为什么在**镜像里**预建而不是在 compose 里 chown：
#   ① Docker 对**空具名卷**的初始化语义：卷被挂载到容器内一个**已存在且非空**的目录时，
#      该目录的**内容会被复制进卷**（官方文档 Volumes 一节：
#      "If you start a container which creates a new volume, and the container has
#       files or directories in the directory to be mounted, Docker copies the
#       directory's contents into the volume."）。属主/权限随目录内容一并保留。
#      ⚠️ 本条只引用官方那句话的**语义**；本仓没有逐字保留该句的原始出处链接，
#      故按 HLD §0.3 **不标 `官方给出`** —— 它是「官方文档表述 + 本仓实测」的合成结论。
#      故镜像里预建 9999:9999 的 /warehouse ⇒ 新卷天然可写。
#      实机已验证：全新具名卷 + 本镜像 ⇒ `ls -ldn` = `9999 9999`，写入成功。
#   ② compose 侧没有 chown 的声明式入口；用 entrypoint 包装会引入额外 shell 层，
#      且 JM/TM 两处都要各自处理 ⇒ 不如在镜像里一次性固化。
#   ⚠️ **只对新建卷生效**：已存在的卷（属主 root）不会因重建镜像而改变属主。
#      本项目 P2 首次启用的卷都是新建，故本条足够；若日后遇到继承下来的 root 卷，
#      需手工 `docker run --rm -v <vol>:/w alpine chown -R 9999:9999 /w` 修一次。
#
# 数字说明（严格按 HLD §0.3 的标签定义）：
#   uid/gid **9999** = `设计值`，**不是 `官方给出`**。
#     理由：HLD §0.3 把 `官方给出` 定义为「有官方文档 / 源码 / 配置文件的**逐字出处**」。
#     9999 这个值本仓**拿不到官方逐字声明**——它是我们对镜像实测（容器内 `id` →
#     uid=9999(flink) gid=9999(flink)）得到的**观测事实**，随镜像升级可能漂移。
#     故按「官方未给出 ⇒ 不得当官方保证」处理，grep 认可其为设计值。
#   chmod **755** = `设计值`（仅需目录可遍历 + 属主可写；Paimon 不需要 group/other 写）。
#   ⚠️ 升 Flink 版本或换基础镜像时必须复核 flink 用户的 uid，硬编码有漂移风险。
USER root
RUN set -eux; \
    mkdir -p /warehouse; \
    chown -R 9999:9999 /warehouse; \
    chmod 755 /warehouse; \
    ls -ldn /warehouse
USER flink
