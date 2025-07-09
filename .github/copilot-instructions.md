# GitHub Copilot Instructions for Jogasaki

## Project Overview
Jogasaki is a high-performance SQL execution engine for the Tsurugi database system. It implements a distributed, parallel query execution framework with advanced memory management, transaction support, and pluggable storage abstraction.

## Architecture & Key Components

### Core Subsystems
- **Public API Layer** (`include/jogasaki/api/`, `src/jogasaki/api/`): Database operations, executable statements, transaction handling
- **Executor** (`src/jogasaki/executor/`): Query execution engine with operators (scan, join, aggregate), exchange operators, and expression evaluation
- **Scheduler** (`src/jogasaki/scheduler/`): Task scheduling with multiple implementations (serial, stealing, hybrid)
- **Memory Management** (`src/jogasaki/memory/`): NUMA-aware page pool with specialized allocators (FIFO, LIFO, monotonic)
- **Storage Abstraction** (`src/jogasaki/kvs/`): Key-value storage wrapper with transaction support
- **Metadata & Planning** (`src/jogasaki/meta/`, `src/jogasaki/plan/`): Record metadata, type system, SQL compilation

### Design Patterns
- **Asynchronous Execution**: All major operations use async callbacks to prevent thread pool exhaustion
- **NUMA-Aware Memory**: Per-NUMA-node memory pools with worker thread local allocation
- **Pluggable Storage**: Supports `memory` (testing) and `shirakami` (production) backends

## Code Style & Standards

### Language & Conventions
- **C++17** standard with extensions disabled
- **Namespace**: Use `jogasaki` for main components
- **Memory Management**: Prefer RAII and smart pointers
- **Error Handling**: Use `jogasaki::error` namespace for error codes
- **Async Operations**: Implement proper error propagation through callbacks

### File Organization
- **Headers**: `include/jogasaki/` for public API, `src/jogasaki/` for implementation
- **Tests**: `test/` with `*_test.cpp` pattern using Google Test
- **Examples**: `examples/` with CLI tools and applications

## Common Patterns to Follow

### Memory Allocation
```cpp
// Use paged memory resources for large allocations
auto resource = std::make_shared<jogasaki::memory::fifo_paged_memory_resource>();
// Leverage NUMA-aware allocation patterns
```

### Error Handling
```cpp
// Use jogasaki error namespace
using jogasaki::error::error_code;
// Handle KVS errors with utility functions
handle_kvs_errors(status, callback);
```

### Transaction Management
```cpp
// Always use proper transaction scoping
auto tx = database->create_transaction();
// Handle states: active, committed, aborted
// Use async operations where possible
```

### Testing
```cpp
// Follow existing test patterns
#include <gtest/gtest.h>
// Use test utilities in test/ directory
// Tests should be deterministic and timing-independent
```

## Build System

### Dependencies
- **Core Libraries**: takatori, yugawara, mizugaki, sharksfin, limestone
- **System**: Boost, TBB, glog, gflags, protobuf, mpdecimal
- **Data**: Apache Arrow/Parquet (version 16.1.0-1)
- **Testing**: Google Test (submodule)

### Common Build Commands
```bash
# Development build
mkdir -p build && cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DBUILD_EXAMPLES=ON ..
cmake --build .

# Run tests
ctest -V
```

### Storage Backend Selection
- Use `-DSHARKSFIN_IMPLEMENTATION=<memory|shirakami>` 
- Default: `shirakami` (production)
- `memory` backend for testing only

## Performance Considerations

### Memory Usage
- Be mindful of allocation patterns (FIFO vs LIFO vs monotonic)
- Use appropriate memory resource types
- Consider NUMA topology for multi-socket systems

### Parallel Execution
- Implement proper coordination to avoid contention
- Use work-stealing scheduler for load balancing
- Consider task granularity for optimal performance

### Storage Operations
- Use async callbacks for non-blocking operations
- Implement proper transaction retry logic
- Handle storage backend differences appropriately

## Common Code Patterns

### Async Operations
```cpp
// Typical async pattern
void operation(callback_type callback) {
    // Perform operation
    callback(result);
}
```

### Memory Resource Usage
```cpp
// Use appropriate memory resource
auto resource = get_memory_resource();
auto ptr = resource->allocate(size);
```

### Error Propagation
```cpp
// Proper error handling
if (auto res = operation(); !res) {
    return handle_error(res.error());
}
```

## Testing Guidelines

### Test Structure
- Use Google Test framework
- Follow `*_test.cpp` naming convention
- Include comprehensive edge case testing
- Ensure tests are deterministic

### Test Categories
- **Unit Tests**: Individual component testing
- **Integration Tests**: Cross-component functionality
- **Performance Tests**: Benchmarking critical paths

## Documentation
- Use Doxygen-style comments for public APIs
- Include usage examples in complex components
- Document async callback patterns clearly
- Explain memory management requirements

## Common Pitfalls to Avoid

1. **Memory Leaks**: Always use RAII patterns
2. **Thread Safety**: Be careful with shared state
3. **Transaction Lifetimes**: Ensure proper cleanup
4. **Error Handling**: Don't ignore async callback errors
5. **Performance**: Avoid unnecessary allocations in hot paths

## Useful Utilities

### Debug/Development
- `jogasaki::utils::trace_log` for debugging
- `jogasaki::utils::performance_tools` for profiling
- Storage dump utilities for debugging

### String/Data Conversion
- `jogasaki::utils::convert_any` for type conversion
- `jogasaki::utils::string_utils` for string operations
- `jogasaki::utils::hex_to_octet` for binary data

### Validation
- `jogasaki::utils::validate_table_definition`
- `jogasaki::utils::validate_index_key_type`
- `jogasaki::utils::validate_any_type`

This project emphasizes high performance, correctness, and maintainability. When suggesting code, prioritize thread safety, proper resource management, and adherence to the established async patterns.