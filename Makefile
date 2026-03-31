.PHONY: build test test-unit test-s3 bench clean

BUILD_DIR ?= build
CMAKE_FLAGS ?= -DCMAKE_BUILD_TYPE=Debug

# Detect Homebrew LLVM on macOS.
UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
  ifneq ($(wildcard /opt/homebrew/opt/llvm/bin/clang++),)
    CMAKE_FLAGS += -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm/bin/clang \
                   -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++
  endif
endif

build:
	cmake -B $(BUILD_DIR) $(CMAKE_FLAGS) .
	cmake --build $(BUILD_DIR) -- -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu)

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure -R "ChunkCodec|Manifest|PageBitmap|PageIO|Prefetch[^S]|VFS[^S]"

test-unit: test

# S3 integration tests (requires TIGRIS_STORAGE_* env vars).
test-s3: build
	cd $(BUILD_DIR) && ctest --output-on-failure -R "S3|PrefetchS3|VFSS3|RangeRequest"

# Full benchmark (requires LadybugDB).
bench:
	cmake -B $(BUILD_DIR) $(CMAKE_FLAGS) -DBUILD_BENCH=ON \
		-DLADYBUG_DIR=$(LADYBUG_DIR) .
	cmake --build $(BUILD_DIR) --target cypher_bench -- -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu)

# Deploy benchmark to Fly.
deploy-bench:
	cd ../.. && fly deploy --config turbograph/bench/fly.toml --remote-only

clean:
	rm -rf $(BUILD_DIR)

help:
	@echo "make build        - Build library + tests"
	@echo "make test         - Run unit tests (no S3 needed)"
	@echo "make test-s3      - Run S3 integration tests"
	@echo "make bench        - Build benchmark (needs LADYBUG_DIR)"
	@echo "make deploy-bench - Deploy benchmark to Fly"
	@echo "make clean        - Remove build dir"
