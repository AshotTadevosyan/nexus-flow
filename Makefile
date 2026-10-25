BUILD_DIR = build
CMAKE ?= /opt/homebrew/bin/cmake

.PHONY: all clean test run-server run-client benchmark

all: $(BUILD_DIR)/Makefile
	@$(CMAKE) --build $(BUILD_DIR) -j

$(BUILD_DIR)/Makefile:
	@mkdir -p $(BUILD_DIR)
	@$(CMAKE) -B $(BUILD_DIR) -S . -DCMAKE_BUILD_TYPE=Release

test: all
	@echo "===> Running automated tests..."
	@cd $(BUILD_DIR) && ctest --output-on-failure

run-server: all
	@echo "===> Launching NexusFlow Broker Server..."
	@./$(BUILD_DIR)/nexus_server --port 9090

run-client: all
	@./$(BUILD_DIR)/nexus_cli 127.0.0.1 9090

benchmark: all
	@echo "===> Executing Benchmark Suite..."
	@./$(BUILD_DIR)/nexus_benchmark --clients 20 --messages 5000 --subscribers 4 --port 9090

clean:
	@rm -rf $(BUILD_DIR)
