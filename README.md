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
  * tsubakuro (for proto definition)
  * performance-tools (optional)
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-filesystem-dev libboost-system-dev libboost-container-dev libboost-thread-dev libboost-stacktrace-dev libgoogle-glog-dev libgflags-dev doxygen libtbb-dev libnuma-dev protobuf-compiler protobuf-c-compiler libprotobuf-dev libmsgpack-dev uuid-dev libicu-dev pkg-config flex bison
```

optional packages:

* `doxygen`
* `graphviz`
* `clang-tidy-8`
* [`linenoise-ng`](https://github.com/arangodb/linenoise-ng.git)

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
* `-DINSTALL_API_ONLY=ON` - configure build directory just to install public header files. Use when other components require jogasaki public headers.
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DTRACY_ENABLE=ON` - enable tracy profiler for multi-thread debugging. See section below.
  * `-DLIKWID_ENABLE=ON` - enable LIKWID for performance metrics. See section below.
    
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

(2021-11 TracyClient.cpp is included in tateyama only in order to avoid start up conflict, so set `-DTRACY_ENABLE=ON` both on tateyama and jogasaki if you profile jogasaki)

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

### Recompile time saving with ccache

You can save re-compile time using [ccache](https://ccache.dev), which caches the compiled output and reuse on recompilation if no change is detected.
This works well with jogasaki build as jogasaki build has dependencies to many components and translation units are getting bigger. 

1. Install ccache 
```
apt install ccache
```
2. add `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache` to cmake build option. 

First time build does not change as it requires building and caching all artifacts into cache directory, e.g. `~/.ccache`. When you recompile, you will see it finishes very fast.
Checking `ccache -s` shows the cache hit ratio and size of the cached files.

### Profiling with LIKWID

You can use [LIKWID](https://github.com/RRZE-HPC/likwid) to retrieve the performance metrics via hardware counters.
By setting cmake build option `-DLIKWID_ENABLE=ON`, jogasaki is linked to the LIKWID library and its marker API macros are enabled.

Prerequirement:

1. Install LIKWID in your environment. Typically, this can be done by clone the LIKWID repository, update the config.mk, run `make` and `make install`. You can install to users local directory, but you need `sudo` to run `make install` in order to set SUID for some binary files.

2. include common.h at the top of files that requires profiling. This allow you to call LIKWID marker APIs such as `LIKWID_MARKER_START`
```
#include <jogasaki/common.h>
```
3. Make sure LIKWID initialize/deinitialize macros `LIKWID_MARKER_INIT`/`LIKWID_MARKER_CLOSE` are called at some point where the code does initialize/deinitialize. 
4. Put `LIKWID_MARKER_START`/`LIKWID_MARKER_STOP` macros to specify the scope to profile. 

Running jogasaki with likwid-perfctr command will show you the performance counters incremented by the code between`LIKWID_MARKER_START` and `LIKWID_MARKER_STOP`. See LIKWID documentation for details

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

