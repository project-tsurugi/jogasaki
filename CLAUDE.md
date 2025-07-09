# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Jogasaki is a high-performance SQL execution engine for the Tsurugi database system. It implements a distributed, parallel query execution framework with advanced memory management, transaction support, and pluggable storage abstraction.

## Build System & Common Commands

### Build Commands
```bash
# Basic build
mkdir -p build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
cmake --build .

# Development build with tests and examples
mkdir -p build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DBUILD_EXAMPLES=ON -DBUILD_DOCUMENTS=ON ..
cmake --build .
```

### Test Commands
```bash
# Run all tests
cd build
ctest -V

# Run specific test
cd build
ctest -V -R test_name

# Run single test executable directly
cd build
./test/specific_test_name
```

### Build Options
- `-DSHARKSFIN_IMPLEMENTATION=<memory|shirakami>` - Switch storage backend (default: shirakami)
- `-DBUILD_TESTS=ON` - Build test programs
- `-DBUILD_EXAMPLES=ON` - Build example applications
- `-DBUILD_DOCUMENTS=ON` - Build documentation with doxygen
- `-DPERFORMANCE_TOOLS=ON` - Enable performance tooling
- `-DENABLE_SANITIZER=ON/OFF` - Enable/disable sanitizers for debug builds
- `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache` - Use ccache for faster rebuilds

### Dependencies
The project requires several external dependencies managed through CMake:
- **Core**: takatori, yugawara, mizugaki, sharksfin, limestone
- **System**: Boost, TBB, glog, gflags, protobuf, mpdecimal
- **Data**: Apache Arrow/Parquet (version 16.1.0-1)
- **Testing**: Google Test (included as submodule)

## Architecture Overview

### Core Components

#### 1. Public API Layer (`include/jogasaki/api/`)
- **`database`**: Main entry point for database operations
- **`executable_statement`**: Compiled SQL statements ready for execution
- **`transaction_handle`**: Transaction lifecycle management
- **`result_set`** and **`data_channel`**: Query result access

#### 2. Executor Subsystem (`src/jogasaki/executor/`)
- **`executor`**: Main execution coordinator with async operations
- **`common::step`** and **`common::task`**: Base execution abstractions
- **`process::processor`**: Specific query operators (scan, join, aggregate)
- **Exchange operators** (`exchange/`): Data movement between execution stages

#### 3. Scheduler (`src/jogasaki/scheduler/`)
- **`statement_scheduler`**: High-level SQL statement execution
- **`task_scheduler`**: Multiple implementations (serial, stealing, hybrid)
- **`transaction_context`**: Transaction wrapper with worker thread management

#### 4. Memory Management (`src/jogasaki/memory/`)
- **`page_pool`**: NUMA-aware 2MB page allocator
- **Specialized allocators**: FIFO, LIFO, monotonic allocation patterns
- **`paged_memory_resource`**: Base class for page-based allocation

#### 5. Storage Abstraction (`src/jogasaki/kvs/`)
- **`database`**: KVS database wrapper
- **`transaction`**: KVS transaction wrapper with callback support
- **`storage`**: Table/index storage representation
- **`iterator`**: Cursor-based data access

#### 6. Metadata & Planning (`src/jogasaki/meta/`, `src/jogasaki/plan/`)
- **`record_meta`**: Record metadata and binary layout
- **`field_type`**: SQL data type system
- **`compiler`**: SQL compilation and planning

### Key Design Patterns

#### Asynchronous Execution
- All major operations support async callbacks
- Non-blocking execution prevents thread pool exhaustion
- Callback-based completion notifications

#### Pluggable Scheduling
- Multiple scheduler implementations for different workloads
- Work-stealing for load balancing
- Configurable execution modes

#### NUMA-Aware Memory Management
- Per-NUMA-node memory pools
- Worker threads prefer local memory allocation
- Configurable NUMA policies

## File Organization

### Source Structure
- `src/jogasaki/` - Main implementation
  - `api/` - Public API implementations
  - `executor/` - Query execution engine
  - `scheduler/` - Task scheduling and parallel execution
  - `memory/` - Memory management subsystem
  - `kvs/` - Key-value storage abstraction
  - `meta/` - Metadata and type system
  - `plan/` - SQL compilation and planning
  - `utils/` - Utility functions and helpers

### Include Structure
- `include/jogasaki/` - Public headers
  - `api/` - Public API definitions
  - `lob/` - Large object support
  - `utils/` - Public utility functions

### Testing
- `test/` - Test implementations using Google Test
- `test/main.cpp` - Common test entry point
- Individual test files follow `*_test.cpp` pattern

### Examples
- `examples/` - Example applications and CLI tools
- `examples/sql_cli/` - Interactive SQL command line interface
- `examples/service_cli/` - Service-level operations

## Development Guidelines

### Code Style
- C++17 standard with extensions disabled
- Follow existing naming conventions in the codebase
- Use namespace `jogasaki` for main components
- Prefer RAII and smart pointers for resource management

### Memory Management
- Use the provided paged memory resources for large allocations
- Leverage NUMA-aware allocation when possible
- Be mindful of memory usage patterns (FIFO vs LIFO vs monotonic)

### Error Handling
- Use the `jogasaki::error` namespace for error codes
- Implement proper error propagation through async callbacks
- Handle KVS errors appropriately using `handle_kvs_errors()`

### Testing
- Write comprehensive tests for new functionality
- Use the provided test utilities in `test/` directory
- Follow the existing test patterns and naming conventions
- Tests should be deterministic and not rely on timing

### Transaction Management
- Always use proper transaction scoping
- Handle transaction states correctly (active, committed, aborted)
- Use async transaction operations where possible
- Implement proper cleanup in error paths

## Important Notes

### Storage Backend
- The system supports two storage backends: `memory` (for testing) and `shirakami` (production)
- Storage backend is selected at build time via `-DSHARKSFIN_IMPLEMENTATION`
- Most development should be done with the `shirakami` backend

### Performance Considerations
- The system is designed for high-performance OLTP workloads
- Memory allocation patterns significantly impact performance
- NUMA awareness is crucial for multi-socket systems
- Parallel execution requires careful coordination to avoid contention

### Dependencies
- The project has complex dependency relationships managed through CMake
- Always use `git submodule update --init --recursive` after cloning
- External dependencies have specific version requirements (especially Apache Arrow/Parquet)

## Troubleshooting

### Common Build Issues
- Missing dependencies: Check README.md for required packages
- Arrow/Parquet version conflicts: Ensure exact version 16.1.0-1 is installed
- Submodule issues: Run `git submodule update --init --recursive`

### Runtime Issues
- Memory allocation failures: Check NUMA configuration and available memory
- Transaction conflicts: Review transaction isolation levels and retry logic
- Storage backend issues: Verify correct backend selection and configuration