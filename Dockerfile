# 最小运行镜像:ubuntu:24.04(提供二进制所需的 glibc >= 2.39)+ 单个二进制。
# 无 RUN 步骤:不装任何包、不产生额外层,buildx 也无需 QEMU 即可交叉构建多架构。
#
# 镜像由 .github/workflows/release.yml 的 docker job 在推送 tag 时构建并推送到
# ghcr.io/42tr/diting:<version> 和 :latest。本地手动构建需先准备两种架构的二进制:
#   mkdir -p docker/linux/amd64 docker/linux/arm64
#   cp <x86_64 二进制>  docker/linux/amd64/diting
#   cp <aarch64 二进制> docker/linux/arm64/diting
#   docker buildx build --platform linux/amd64,linux/arm64 -t diting:local .
FROM ubuntu:24.04
ARG TARGETARCH
WORKDIR /app
# 服务默认监听 127.0.0.1,容器内必须放开到 0.0.0.0
ENV DITING_ADDR=0.0.0.0:3000
COPY docker/linux/${TARGETARCH}/diting /usr/local/bin/diting
EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/diting"]
