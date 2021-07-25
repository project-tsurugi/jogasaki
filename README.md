# Jogasaki - A SQL execution engine 

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* access to installed dependent modules: 
  * sharksfin
  * takatori
  * yugawara
  * mizugaki
  * shakujo (until dependency is removed)
  * fpdecimal
  * performance-tools (optional)
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-filesystem-dev libboost-system-dev libboost-container-dev libboost-thread-dev libboost-stacktrace-dev libgoogle-glog-dev libgflags-dev doxygen libtbb-dev libnuma-dev
```

optional packages:

* `doxygen`
* `graphviz`
* `clang-tidy-8`

## How to build

```sh
mkdir -p build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

available options:
* `-DCMAKE_INSTALL_PREFIX=<installation directory>` - change install location
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequisite installation directory
* `-DCMAKE_IGNORE_PATH="/usr/local/include;/usr/local/lib/"` - specify the libraries search paths to ignore. This is convenient if the environment has conflicting version installed on system default search paths. (e.g. gflags in /usr/local)
* `-DBUILD_SHARED_LIBS=OFF` - create static libraries instead of shared libraries
* `-DBUILD_TESTS=OFF` - don't build test programs
* `-DBUILD_DOCUMENTS=OFF` - don't build documents by doxygen
* `-DINSTALL_EXAMPLES=ON` - install example applications
* `-DFORCE_INSTALL_RPATH=ON` - automatically configure `INSTALL_RPATH` for non-default library paths
* `-DSHARKSFIN_IMPLEMENTATION=<implementation name>` - switch sharksfin implementation. Available options are `memory` and `shirakami` (default: `memory`)
* `-DPERFORMANCE_TOOLS=ON` - enable performance tooling to measure engine performance
* `-DMC_QUEUE=ON` - use moody camel queue instead of tbb queue to store tasks in tateyama task scheduler.
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DUSE_BLOCKING_EVENT_QUEUE=ON` - enable blocking queue for the event channel
  * `-DTRACY_ENABLE=ON` - enable tracy profiler for multi-thread debugging. See section below.

### install 

```sh
cmake --build . --target install
```

### run tests

```sh
ctest -V
```

### generate documents

```sh
cmake --build . --target doxygen
```

### Customize logging setting 
You can customize logging in the same way as sharksfin. See sharksfin [README.md](https://github.com/project-tsurugi/sharksfin/blob/master/README.md#customize-logging-setting) for more details.

```sh
GLOG_minloglevel=0 ./group-cli --minimum 
```

### Multi-thread debugging/profiling with Tracy

You can use [Tracy Profiler](https://github.com/wolfpld/tracy) to graphically display the threads operations and improve printf debug by printing messages on the tooltips on the Tracy profiler UI.
By setting cmake build option `-DTRACY_ENABLE=ON`, TracyClient.cpp file is added to the build and tracing macros are enabled.

Prerequirement: 

1. ensure tracy code is located under `third_party/tracy` directory.
```
git submodule update --init third_party/tracy
```

2. include common.h at the top of files that requires tracing.
```
#include <jogasaki/common.h>
```

3. Put `trace_scope` at the beginning of the scope to trace, or use other tracing functions defined in common.h.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

