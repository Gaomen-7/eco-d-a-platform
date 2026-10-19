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

# 二进制包由构建上下文提供（先下载到 .scratch/doris/）
ARG DORIS_TARBALL=apache-doris-4.1.5-bin-x64.tar.gz
COPY ${DORIS_TARBALL} /tmp/doris.tar.gz
RUN mkdir -p /opt/apache-doris \
 && tar -xzf /tmp/doris.tar.gz -C /opt/apache-doris --strip-components=1 \
 && rm -f /tmp/doris.tar.gz

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

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    DORIS_HOME=/opt/apache-doris \
    PATH=/opt/apache-doris/fe/bin:/opt/apache-doris/be/bin:$PATH

# 官方镜像用 entrypoint 脚本决定起 FE 还是 BE；这里保持与官方 base 一致的约定：
# 由 compose 的 command 指定（fe / be），entrypoint 脚本见 docker/doris/entrypoint.sh
COPY entrypoint.sh /usr/local/bin/doris-entrypoint
RUN chmod +x /usr/local/bin/doris-entrypoint

EXPOSE 8030 9030 9010 8040 9060 9050
ENTRYPOINT ["/usr/local/bin/doris-entrypoint"]
