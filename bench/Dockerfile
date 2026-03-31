FROM debian:trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake make clang g++ libssl-dev libzstd-dev ca-certificates git python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Clone LadybugDB for the full Kuzu library.
# Pin to a specific commit for reproducibility.
COPY ladybug-fork/ /build/ladybug-fork/

# Build LadybugDB (liblbug) first.
RUN cd ladybug-fork && \
    cmake -B build/release \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DBUILD_SHELL=FALSE \
    -DBUILD_SINGLE_FILE_HEADER=FALSE \
    . && \
    cmake --build build/release --target lbug -- -j$(nproc)

# Copy turbograph source.
COPY turbograph/ /build/turbograph/

# Build turbograph + cypher_bench.
# Use shared OpenSSL to avoid static libcrypto's zstd dependency.
RUN cd turbograph && \
    cmake -B build/release \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DBUILD_TESTS=OFF \
    -DBUILD_BENCH=ON \
    -DLADYBUG_DIR=/build/ladybug-fork \
    . && \
    cmake --build build/release --target cypher_bench -- -j$(nproc)

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3t64 libzstd1 libc++1 libatomic1 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/turbograph/build/release/bench/cypher_bench /cypher_bench

ENTRYPOINT ["/cypher_bench"]
CMD ["100000"]
