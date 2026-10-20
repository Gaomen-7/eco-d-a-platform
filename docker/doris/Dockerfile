# Doris 4.1.5 镜像（本地构建）—— 本项目自写，官方不发可直接拉取的版本化镜像
#
# ★ 为什么必须自建（P0 实测，2026-10-19）：
#   官方镜像站**没有**可直接拉取的 Doris 4.1.5：
#     apache/doris:4.1.5 / all-in-one-4.1.5 / base-4.1.5  → 全部 `denied`
#     apache/doris:base-latest                            → 可拉，但实测是**构建基座**
#        （`/opt/apache-doris` 存在，`fe/bin` 为空、容器内**无 java**）⇒ 不含可运行 FE/BE
#   这与官方 `docker/runtime` 的做法一致：**用二进制包自建镜像**。
#
# ★ 二进制包来源（实测 HTTP 200，4.32 GB）：
#   https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-4.1.5-bin-x64.tar.gz
#   （另有 -noavx2 变体，同样 200；本机 CPU 支持 AVX2，故用默认 avx2 版）
#
# ★ 两个出厂默认**必须覆盖**（否则击穿天花板，见 HLD §8-R1）：
#   - FE `fe.conf` 的 `JAVA_OPTS_FOR_JDK_17` 出厂 `-Xmx8192m`（＝整个 8 GB 预算）
#   - BE `be.conf` 的 `mem_limit` 出厂 `90%`（天花板下 ≈10.5 GB）
#
# 构建：
#   docker build -f docker/doris/Dockerfile -t eco/doris:4.1.5 docker/doris
#
# 回滚：删除镜像（docker rmi eco/doris:4.1.5）与 .scratch/doris/ 下的下载产物。

FROM docker.m.daocloud.io/apache/doris:base-latest

# ★ 官方要求 JDK 恰为 17（`start_fe.sh` 非 17 直接 exit 1）。
#   **P0 实测：base-latest 镜像内已自带 `/usr/lib/jvm/jdk-17`**
#   （`java -version` = openjdk 17.0.2 实测可用）⇒ **无需 apt 装 JDK**，
#   直接指过去即可。这既省构建时间，也避免 apt 源不可达的风险。
#   （最初版本的 Dockerfile 里写了一条多余的 `apt-get install openjdk-17`，
#     会让 4 GB 上下文的构建多花很久，已删除。）
ENV JAVA_HOME=/usr/lib/jvm/jdk-17 \
    PATH=/usr/lib/jvm/jdk-17/bin:$PATH

# ── 二进制包：**构建内下载**（P0 改道，2026-10-20）─────────────────────────
# ★ 为什么不再用 `COPY` 4.03 GiB 构建上下文（上一会话实测失败路径）：
#   上下文传输 **3276 秒（54 分钟）**后在 `COPY` 阶段 `EOF`，Docker Desktop 被压垮退出；
#   `docker buildx du` 里留下两条 Doris COPY 缓存层（[2/7] 3.401 GB、[3/8] 4.323 GB）。
#   改道后下载走**容器内**网络（VM 内部，不经宿主↔WSL 文件系统），实测 **7.4–8.9 MB/s**
#   ⇒ 4.32 GB ≈ 8–10 分钟，且产物落在 Docker VM 磁盘（F: 余 52 GB），不挤 C:（余 13 GB）。
#
# ★ 单条 `RUN` 完成「下载 + 解压 + 清理」——拆成多条会残留 ~4 GB 中间层。
# ★★ `tar -xzf` 而非 `-xJ`：该文件是 **gzip**（`Content-Type: application/gzip`，
#    实测 HTTP 200 / Content-Length=4323043285）；`-J` 是 xz，会直接失败。★★
ARG DORIS_TARBALL_URL=https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-4.1.5-bin-x64.tar.gz
RUN set -eux; \
    curl -fsSL -o /tmp/doris.tar.gz "$DORIS_TARBALL_URL"; \
    mkdir -p /opt/apache-doris; \
    tar -xzf /tmp/doris.tar.gz -C /opt/apache-doris --strip-components=1; \
    rm -f /tmp/doris.tar.gz; \
    test -x /opt/apache-doris/fe/bin/start_fe.sh; \
    test -x /opt/apache-doris/be/bin/start_be.sh

# ★ 覆盖 FE 出厂默认堆（8192m → 1536m）。按 HLD §8-R1 / gdd §A.2 的设计值。
RUN set -eux; \
    f=/opt/apache-doris/fe/conf/fe.conf; \
    sed -i 's/-Xmx8192m -Xms8192m/-Xmx1536m -Xms1536m/' "$f"; \
    grep -q 'Xmx1536m' "$f"

# ★ 覆盖 BE 出厂默认 mem_limit（90% → 25%）
RUN set -eux; \
    b=/opt/apache-doris/be/conf/be.conf; \
    if grep -qE '^[[:space:]]*mem_limit' "$b"; then \
      sed -i 's/^[[:space:]]*mem_limit.*/mem_limit = 25%/' "$b"; \
    else \
      printf '\n# 本项目覆盖：出厂默认 90%%（天花板下约 10.5 GB）\nmem_limit = 25%%\n' >> "$b"; \
    fi; \
    grep -E '^[[:space:]]*mem_limit' "$b"

# ★★ Bug E（致命，P0 实测 2026-10-20）——**不设这一条，BE 必然 exit 1，根本起不来** ★★
#   `be/bin/start_be.sh:194-216` 在起进程**之前**做三项前置检查：
#     if [[ "${SKIP_CHECK_ULIMIT:- "false"}" != "true" ]]; then
#       (1) vm.max_map_count 须 ≥ 2000000
#       (2) `swapon -s | wc -l` 须 ≤ 1（有 swap 就拒绝）
#       (3) ulimit -n 须 ≥ 60000
#     任一不过即 `exit 1`
#   本机 Docker Desktop VM 内**实测复现**：
#     vm.max_map_count = **1048576**（< 2000000 ⇒ exit 1）
#     swapon -s 行数   = **2**（/dev/sdc，VM 有 2 GB swap ⇒ exit 1）
#     ulimit -n        = 1048576（≥ 60000，此项通过）
#   而 (1) vm.max_map_count **不是 namespaced sysctl**——容器内改不了，只能跳过检查；
#   (2) 的 swap 同理。故必须在此显式设 true。base-latest 镜像的 ENV **只有 PATH**，
#   没有这个变量（`docker inspect` 实测）⇒ 不设即等于 false。
#   依据（一手）：官方 `docker/runtime/all-in-one/4.1/Dockerfile` 逐字设
#     `SKIP_CHECK_ULIMIT=true`，并注明 "vm.max_map_count is not a namespaced sysctl,
#     so a container cannot fix it -- the check has to be skipped here."
#   ⚠️ FE 无此检查（`grep -n 'max_map_count|swapon|ulimit' fe/bin/start_fe.sh` ⇒ 零命中），
#      所以只需 BE 侧依赖它。
ENV SKIP_CHECK_ULIMIT=true

# ★ 此处**不要**再写 JAVA_HOME！本行曾写 `/usr/lib/jvm/java-17-openjdk-amd64`，
#   P0 实测该路径在 base-latest 里**根本不存在**（`/usr/lib/jvm/` 下只有 `jdk-17` 与 `jdk-8`），
#   且 ENV 是后者覆盖前者 ⇒ 会把上面第 31 行正确的 `jdk-17` 覆盖成坏路径，
#   导致 `start_fe.sh` 找不到 java、FE 必然起不来。JAVA_HOME 只在上方定义一次。
ENV DORIS_HOME=/opt/apache-doris \
    PATH=/opt/apache-doris/fe/bin:/opt/apache-doris/be/bin:$PATH

# 官方镜像用 entrypoint 脚本决定起 FE 还是 BE；这里保持与官方 base 一致的约定：
# 由 compose 的 command 指定（fe / be），entrypoint 脚本见 docker/doris/entrypoint.sh
COPY entrypoint.sh /usr/local/bin/doris-entrypoint
RUN chmod +x /usr/local/bin/doris-entrypoint

EXPOSE 8030 9030 9010 8040 9060 9050
ENTRYPOINT ["/usr/local/bin/doris-entrypoint"]
