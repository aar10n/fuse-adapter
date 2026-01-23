# fuse-adapter Makefile
#
# Helpful recipes for development, testing, and running connectors

.PHONY: help build release test clean
.PHONY: minio-start minio-stop minio-clean minio-setup minio-logs minio-shell minio-ensure-bucket
.PHONY: localstack-start localstack-stop localstack-setup localstack-logs localstack-ensure-bucket
.PHONY: run-s3 run-s3-localstack run-s3-release
.PHONY: mount-dirs unmount test-s3 test-read test-write
.PHONY: test-integration test-integration-quick test-integration-ci
.PHONY: test-e2e test-e2e-s3 test-e2e-cache test-e2e-quick bench bench-read bench-write bench-metadata

# Configuration
MINIO_CONTAINER := fuse-adapter-minio
LOCALSTACK_CONTAINER := fuse-adapter-localstack
MINIO_ROOT_USER := minioadmin
MINIO_ROOT_PASSWORD := minioadmin
MINIO_PORT := 9000
MINIO_CONSOLE_PORT := 9001
LOCALSTACK_PORT := 4566
TEST_BUCKET := test-bucket
MOUNT_BASE := /tmp/fuse-adapter
S3_MOUNT := $(MOUNT_BASE)/s3
CACHE_DIR := $(MOUNT_BASE)/cache

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

#-----------------------------------------------------------------------------
# Help
#-----------------------------------------------------------------------------

help: ## Show this help
	@echo "fuse-adapter development commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Build:"
	@grep -E '^(build|release|test|clean):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "MinIO (S3-compatible):"
	@grep -E '^minio-(start|stop|clean|setup|logs|shell|status):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "LocalStack (AWS emulator):"
	@grep -E '^localstack-.*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "Run:"
	@grep -E '^run-.*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "Test:"
	@grep -E '^test-.*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "Integration Tests:"
	@grep -E '^test-(integration|all):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "Utilities:"
	@grep -E '^(mount-dirs|unmount):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

#-----------------------------------------------------------------------------
# Build
#-----------------------------------------------------------------------------

build: ## Build debug version
	cargo build

release: ## Build release version
	cargo build --release

test: ## Run all tests
	cargo test

clean: ## Clean build artifacts
	cargo clean
	rm -rf $(MOUNT_BASE)

#-----------------------------------------------------------------------------
# MinIO
#-----------------------------------------------------------------------------

minio-start: ## Start MinIO container
	@echo "$(GREEN)Starting MinIO...$(NC)"
	@docker rm -f $(MINIO_CONTAINER) 2>/dev/null || true
	docker volume create $(MINIO_CONTAINER)-data >/dev/null 2>&1 || true
	docker run -d \
		--name $(MINIO_CONTAINER) \
		-p $(MINIO_PORT):9000 \
		-p $(MINIO_CONSOLE_PORT):9001 \
		-v $(MINIO_CONTAINER)-data:/data \
		-e MINIO_ROOT_USER=$(MINIO_ROOT_USER) \
		-e MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) \
		minio/minio server /data --console-address ":9001"
	@echo "$(GREEN)MinIO started$(NC)"
	@echo "  API:     http://localhost:$(MINIO_PORT)"
	@echo "  Console: http://localhost:$(MINIO_CONSOLE_PORT)"
	@echo "  User:    $(MINIO_ROOT_USER)"
	@echo "  Pass:    $(MINIO_ROOT_PASSWORD)"
	@echo "  Volume:  $(MINIO_CONTAINER)-data"

minio-stop: ## Stop MinIO container (keeps data)
	@echo "$(YELLOW)Stopping MinIO...$(NC)"
	docker stop $(MINIO_CONTAINER) 2>/dev/null || true
	docker rm $(MINIO_CONTAINER) 2>/dev/null || true
	@echo "$(GREEN)MinIO stopped (data preserved in volume)$(NC)"

minio-clean: ## Stop MinIO and delete all data
	@echo "$(YELLOW)Stopping MinIO and removing data...$(NC)"
	docker stop $(MINIO_CONTAINER) 2>/dev/null || true
	docker rm $(MINIO_CONTAINER) 2>/dev/null || true
	docker volume rm $(MINIO_CONTAINER)-data 2>/dev/null || true
	@echo "$(GREEN)MinIO stopped and data removed$(NC)"

minio-setup: ## Create test bucket in MinIO
	@echo "$(GREEN)Setting up MinIO bucket...$(NC)"
	@echo "Waiting for MinIO to be ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -sf http://localhost:$(MINIO_PORT)/minio/health/live >/dev/null 2>&1; then \
			echo "MinIO is ready"; \
			break; \
		fi; \
		echo "  Attempt $$i: waiting..."; \
		sleep 1; \
	done
	@echo "Creating bucket '$(TEST_BUCKET)'..."
	@if docker ps --filter name=$(MINIO_CONTAINER) --format "{{.Names}}" | grep -q $(MINIO_CONTAINER); then \
		docker exec $(MINIO_CONTAINER) mc alias set local http://localhost:9000 $(MINIO_ROOT_USER) $(MINIO_ROOT_PASSWORD) && \
		docker exec $(MINIO_CONTAINER) mc mb --ignore-existing local/$(TEST_BUCKET); \
	elif command -v mc >/dev/null 2>&1; then \
		mc alias set local http://localhost:$(MINIO_PORT) $(MINIO_ROOT_USER) $(MINIO_ROOT_PASSWORD) && \
		mc mb --ignore-existing local/$(TEST_BUCKET); \
	elif command -v aws >/dev/null 2>&1; then \
		AWS_ACCESS_KEY_ID=$(MINIO_ROOT_USER) AWS_SECRET_ACCESS_KEY=$(MINIO_ROOT_PASSWORD) \
		aws --endpoint-url=http://localhost:$(MINIO_PORT) s3 mb s3://$(TEST_BUCKET) 2>/dev/null || true; \
	else \
		echo "$(RED)No mc or aws CLI available to create bucket$(NC)" && exit 1; \
	fi
	@echo "$(GREEN)Bucket '$(TEST_BUCKET)' ready$(NC)"

minio-logs: ## Show MinIO container logs
	docker logs -f $(MINIO_CONTAINER)

minio-shell: ## Open shell in MinIO container
	docker exec -it $(MINIO_CONTAINER) /bin/sh

minio-status: ## Check MinIO status
	@docker ps --filter name=$(MINIO_CONTAINER) --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | head -2 || echo "$(RED)MinIO not running$(NC)"

minio-ensure-bucket: ## Ensure MinIO is running and bucket exists
	@if curl -sf http://localhost:$(MINIO_PORT)/minio/health/live >/dev/null 2>&1; then \
		echo "$(GREEN)MinIO already running on port $(MINIO_PORT)$(NC)"; \
	elif docker ps --filter name=$(MINIO_CONTAINER) --format "{{.Names}}" | grep -q $(MINIO_CONTAINER); then \
		echo "$(GREEN)MinIO container running$(NC)"; \
	else \
		echo "$(YELLOW)MinIO not running, starting...$(NC)"; \
		$(MAKE) minio-start; \
		sleep 2; \
	fi
	@$(MAKE) minio-setup

#-----------------------------------------------------------------------------
# LocalStack
#-----------------------------------------------------------------------------

localstack-start: ## Start LocalStack container
	@echo "$(GREEN)Starting LocalStack...$(NC)"
	@docker rm -f $(LOCALSTACK_CONTAINER) 2>/dev/null || true
	docker run -d \
		--name $(LOCALSTACK_CONTAINER) \
		-p $(LOCALSTACK_PORT):4566 \
		-e SERVICES=s3 \
		localstack/localstack
	@echo "$(GREEN)LocalStack started$(NC)"
	@echo "  S3 Endpoint: http://localhost:$(LOCALSTACK_PORT)"

localstack-stop: ## Stop LocalStack container
	@echo "$(YELLOW)Stopping LocalStack...$(NC)"
	docker stop $(LOCALSTACK_CONTAINER) 2>/dev/null || true
	docker rm $(LOCALSTACK_CONTAINER) 2>/dev/null || true
	@echo "$(GREEN)LocalStack stopped$(NC)"

localstack-setup: ## Create test bucket in LocalStack
	@echo "$(GREEN)Setting up LocalStack bucket...$(NC)"
	@sleep 3  # Wait for LocalStack to be ready
	aws --endpoint-url=http://localhost:$(LOCALSTACK_PORT) \
		s3 mb s3://$(TEST_BUCKET) --region us-east-1 2>/dev/null || \
		echo "$(YELLOW)Bucket already exists$(NC)"
	@echo "$(GREEN)Bucket '$(TEST_BUCKET)' ready$(NC)"

localstack-logs: ## Show LocalStack container logs
	docker logs -f $(LOCALSTACK_CONTAINER)

localstack-ensure-bucket: ## Ensure LocalStack is running and bucket exists
	@if ! docker ps --filter name=$(LOCALSTACK_CONTAINER) --format "{{.Names}}" | grep -q $(LOCALSTACK_CONTAINER); then \
		echo "$(YELLOW)LocalStack not running, starting...$(NC)"; \
		$(MAKE) localstack-start; \
		sleep 3; \
	fi
	@$(MAKE) localstack-setup

#-----------------------------------------------------------------------------
# Mount directories
#-----------------------------------------------------------------------------

mount-dirs: ## Create mount point directories
	@echo "$(GREEN)Creating directories...$(NC)"
	mkdir -p $(S3_MOUNT)
	mkdir -p $(CACHE_DIR)/s3
	@echo "  Mount:  $(S3_MOUNT)"
	@echo "  Cache:  $(CACHE_DIR)"

unmount: ## Unmount all fuse-adapter mounts
	@echo "$(YELLOW)Unmounting...$(NC)"
	-umount $(S3_MOUNT) 2>/dev/null || true
	-fusermount -u $(S3_MOUNT) 2>/dev/null || true
	-diskutil unmount $(S3_MOUNT) 2>/dev/null || true
	@echo "$(GREEN)Unmounted$(NC)"

#-----------------------------------------------------------------------------
# Run
#-----------------------------------------------------------------------------

run-s3: mount-dirs minio-ensure-bucket ## Run with S3/MinIO config
	@echo "$(GREEN)Starting fuse-adapter with MinIO...$(NC)"
	AWS_ACCESS_KEY_ID=$(MINIO_ROOT_USER) \
	AWS_SECRET_ACCESS_KEY=$(MINIO_ROOT_PASSWORD) \
	cargo run -- config/s3.yaml

run-s3-release: mount-dirs minio-ensure-bucket build-release ## Run release build with S3/MinIO
	@echo "$(GREEN)Starting fuse-adapter (release) with MinIO...$(NC)"
	AWS_ACCESS_KEY_ID=$(MINIO_ROOT_USER) \
	AWS_SECRET_ACCESS_KEY=$(MINIO_ROOT_PASSWORD) \
	./target/release/fuse-adapter config/s3.yaml

run-s3-localstack: mount-dirs localstack-ensure-bucket ## Run with LocalStack config
	@echo "$(GREEN)Starting fuse-adapter with LocalStack...$(NC)"
	AWS_ACCESS_KEY_ID=test \
	AWS_SECRET_ACCESS_KEY=test \
	cargo run -- config/s3-localstack.yaml

#-----------------------------------------------------------------------------
# Test operations
#-----------------------------------------------------------------------------

test-s3: ## Run S3 integration tests (requires running mount)
	@echo "$(GREEN)Testing S3 mount at $(S3_MOUNT)...$(NC)"
	@echo ""
	@echo "1. Listing root directory..."
	ls -la $(S3_MOUNT)
	@echo ""
	@echo "2. Creating test file..."
	echo "Hello from fuse-adapter $$(date)" > $(S3_MOUNT)/test-$$(date +%s).txt
	@echo ""
	@echo "3. Listing after create..."
	ls -la $(S3_MOUNT)
	@echo ""
	@echo "$(GREEN)Basic tests passed!$(NC)"

test-read: ## Test read operations
	@echo "$(GREEN)Testing read operations...$(NC)"
	@if [ -f "$(S3_MOUNT)/test.txt" ]; then \
		echo "Reading test.txt:"; \
		cat $(S3_MOUNT)/test.txt; \
	else \
		echo "$(YELLOW)No test.txt found. Creating one...$(NC)"; \
		echo "Test content" > $(S3_MOUNT)/test.txt; \
		cat $(S3_MOUNT)/test.txt; \
	fi

test-write: ## Test write operations
	@echo "$(GREEN)Testing write operations...$(NC)"
	@TESTFILE=$(S3_MOUNT)/write-test-$$(date +%s).txt; \
	echo "Writing to $$TESTFILE..."; \
	echo "Line 1" > $$TESTFILE; \
	echo "Line 2" >> $$TESTFILE; \
	echo "Line 3" >> $$TESTFILE; \
	echo "Content:"; \
	cat $$TESTFILE; \
	echo ""; \
	echo "Size: $$(wc -c < $$TESTFILE) bytes"

test-stress: ## Run stress test with many files
	@echo "$(GREEN)Running stress test...$(NC)"
	@TESTDIR=$(S3_MOUNT)/stress-test-$$(date +%s); \
	mkdir -p $$TESTDIR; \
	echo "Creating 100 files..."; \
	for i in $$(seq 1 100); do \
		echo "File $$i content" > $$TESTDIR/file-$$i.txt; \
	done; \
	echo "Files created: $$(ls $$TESTDIR | wc -l)"; \
	echo "Cleaning up..."; \
	rm -rf $$TESTDIR; \
	echo "$(GREEN)Stress test complete$(NC)"

#-----------------------------------------------------------------------------
# Quick start
#-----------------------------------------------------------------------------

quickstart: ## Full setup: start MinIO, create bucket, build, and run
	@echo "$(GREEN)=== fuse-adapter Quick Start ===$(NC)"
	@echo ""
	$(MAKE) minio-start
	$(MAKE) minio-setup
	$(MAKE) mount-dirs
	@echo ""
	@echo "$(GREEN)Ready! Starting fuse-adapter...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	@echo ""
	$(MAKE) run-s3

stop-all: unmount minio-stop localstack-stop ## Stop everything
	@echo "$(GREEN)All services stopped$(NC)"

#-----------------------------------------------------------------------------
# Integration Tests
#-----------------------------------------------------------------------------

test-integration: ## Run full integration test suite
	@echo "$(GREEN)Running integration tests...$(NC)"
	./scripts/test-harness.sh

test-integration-quick: ## Run quick integration smoke tests
	@echo "$(GREEN)Running quick integration tests...$(NC)"
	./scripts/test-harness.sh --quick

test-integration-ci: ## Run integration tests in CI mode (MinIO already running)
	@echo "$(GREEN)Running integration tests (CI mode)...$(NC)"
	./scripts/test-harness.sh --ci

test-all: test test-integration ## Run all tests (unit + integration)
	@echo "$(GREEN)All tests complete!$(NC)"

#-----------------------------------------------------------------------------
# E2E Tests (Rust-based)
#-----------------------------------------------------------------------------

.PHONY: test-e2e test-e2e-s3 test-e2e-cache test-e2e-quick bench bench-read bench-write bench-metadata

test-e2e: release minio-ensure-bucket ## Run all Rust e2e tests
	@echo "$(GREEN)Running Rust e2e tests...$(NC)"
	MINIO_ENDPOINT=http://localhost:$(MINIO_PORT) \
	MINIO_ACCESS_KEY=$(MINIO_ROOT_USER) \
	MINIO_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	cargo test -p fuse-adapter-e2e --test '*' -- --test-threads=1

test-e2e-s3: release minio-ensure-bucket ## Run S3 connector e2e tests
	@echo "$(GREEN)Running S3 e2e tests...$(NC)"
	MINIO_ENDPOINT=http://localhost:$(MINIO_PORT) \
	MINIO_ACCESS_KEY=$(MINIO_ROOT_USER) \
	MINIO_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	cargo test -p fuse-adapter-e2e --test 's3_*' -- --test-threads=1

test-e2e-cache: release minio-ensure-bucket ## Run cache e2e tests
	@echo "$(GREEN)Running cache e2e tests...$(NC)"
	MINIO_ENDPOINT=http://localhost:$(MINIO_PORT) \
	MINIO_ACCESS_KEY=$(MINIO_ROOT_USER) \
	MINIO_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	cargo test -p fuse-adapter-e2e --test 'cache_*' -- --test-threads=1

test-e2e-quick: release minio-ensure-bucket ## Run quick e2e smoke tests
	@echo "$(GREEN)Running quick e2e tests...$(NC)"
	MINIO_ENDPOINT=http://localhost:$(MINIO_PORT) \
	MINIO_ACCESS_KEY=$(MINIO_ROOT_USER) \
	MINIO_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	cargo test -p fuse-adapter-e2e --test 's3_crud' -- --test-threads=1

test-e2e-mount-config: release minio-ensure-bucket ## Run mount configuration tests
	@echo "$(GREEN)Running mount configuration e2e tests...$(NC)"
	MINIO_ENDPOINT=http://localhost:$(MINIO_PORT) \
	MINIO_ACCESS_KEY=$(MINIO_ROOT_USER) \
	MINIO_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	cargo test -p fuse-adapter-e2e --test 'mount_config' -- --test-threads=1

test-e2e-fuse: release minio-ensure-bucket ## Run FUSE semantics tests
	@echo "$(GREEN)Running FUSE semantics e2e tests...$(NC)"
	MINIO_ENDPOINT=http://localhost:$(MINIO_PORT) \
	MINIO_ACCESS_KEY=$(MINIO_ROOT_USER) \
	MINIO_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	cargo test -p fuse-adapter-e2e --test 'fuse_semantics' -- --test-threads=1

test-e2e-cache-limits: release minio-ensure-bucket ## Run cache eviction and limits tests
	@echo "$(GREEN)Running cache limits e2e tests...$(NC)"
	MINIO_ENDPOINT=http://localhost:$(MINIO_PORT) \
	MINIO_ACCESS_KEY=$(MINIO_ROOT_USER) \
	MINIO_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	cargo test -p fuse-adapter-e2e --test 'cache_limits' -- --test-threads=1

bench: release minio-ensure-bucket mount-dirs ## Run all benchmarks
	@echo "$(GREEN)Running benchmarks...$(NC)"
	@echo "$(YELLOW)Note: Requires mounted FUSE filesystem at $(S3_MOUNT)$(NC)"
	FUSE_MOUNT_PATH=$(S3_MOUNT) \
	cargo bench -p fuse-adapter-e2e

bench-read: release ## Run read benchmarks
	@echo "$(GREEN)Running read benchmarks...$(NC)"
	FUSE_MOUNT_PATH=$(S3_MOUNT) \
	cargo bench -p fuse-adapter-e2e --bench read

bench-write: release ## Run write benchmarks
	@echo "$(GREEN)Running write benchmarks...$(NC)"
	FUSE_MOUNT_PATH=$(S3_MOUNT) \
	cargo bench -p fuse-adapter-e2e --bench write

bench-metadata: release ## Run metadata benchmarks
	@echo "$(GREEN)Running metadata benchmarks...$(NC)"
	FUSE_MOUNT_PATH=$(S3_MOUNT) \
	cargo bench -p fuse-adapter-e2e --bench metadata
