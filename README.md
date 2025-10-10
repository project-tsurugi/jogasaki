# Jogasaki SQL job scheduler

## Requirements

* CMake `>= 3.16`
* C++ Compiler `>= C++17`
* mpdecimal 2.5.1 (see `Manual install steps for mpdecimal` section below to install on Ubuntu 24.04)
* and see *Dockerfile* section

```sh
# retrieve third party modules
# (Some third-party components are stored as submodules. Initialize those you need.)
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:22.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-filesystem-dev libboost-system-dev libboost-container-dev libboost-thread-dev libboost-stacktrace-dev libgoogle-glog-dev libgflags-dev doxygen libtbb-dev libnuma-dev protobuf-compiler protobuf-c-compiler libprotobuf-dev libmsgpack-dev uuid-dev libicu-dev pkg-config flex bison libmpdec-dev nlohmann-json3-dev libparquet-dev=21.0.0-1 libparquet2100=21.0.0-1 libarrow-dev=21.0.0-1 libarrow2100=21.0.0-1
```
(see "Additional file installation for Apache Arrow/Parquet" below if installing `libparquet-dev`, `libarrow-dev` fails)

optional packages:

* `doxygen`
* `graphviz`
* `clang-tidy-14`
* [`linenoise-ng`](https://github.com/arangodb/linenoise-ng.git)

### Install modules

#### tsurugidb modules

This requires below [tsurugidb](https://github.com/project-tsurugi/tsurugidb) modules to be installed.
* [mizugaki](https://github.com/project-tsurugi/mizugaki)
* [tateyama](https://github.com/project-tsurugi/tateyama)

### Additional file installation for Apache Arrow/Parquet

Jogasaki requires Apache Arrow and Parquet package versioned as `21.0.0-1` (Official release stays to this version for stability. Jogasaki may be built and run with later or older versions, but it's for development/testing purpose only, not for production.)

Installing Apache Arrow/Parquet packages `libarrow-dev`, `libarrow2100`, `libparquet-dev`, `libparquet2100` requires additional files installation.
If installing these packages from `apt install` command fails, issue below commands to install required files.

```
sudo apt install -y -V lsb-release
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libparquet-dev=21.0.0-1 libparquet2100=21.0.0-1 libarrow-dev=21.0.0-1 libarrow2100=21.0.0-1
```

(You can see [here](https://arrow.apache.org/install/) for full instruction. )

### Vendored Arrow/Parquet (third_party/arrow)

Jogasaki supports using a vendored copy of Apache Arrow/Parquet located in the repository under `third_party/arrow`.
This is useful when system packages for Arrow/Parquet are unavailable, or when a reproducible Arrow build is required.

How to enable vendored Arrow

1. Ensure the Arrow sources are present at `third_party/arrow` (the vendored Arrow tree must contain `cpp/CMakeLists.txt`).
  The repository does not require Arrow to be a submodule; any mechanism that places the Arrow sources at `third_party/arrow` is acceptable.
2. Configure CMake with the vendored option and (optionally) explicitly set the Arrow dependency source:
  ```sh
  cmake -S . -B build-vendored -DUSE_VENDORED_ARROW=ON -DARROW_DEPENDENCY_SOURCE=BUNDLED -G Ninja
  ```

What the vendored mode does (defaults)

- The vendored Arrow sources are added via `add_subdirectory(third_party/arrow/cpp ...)` so Arrow is built as part of the top-level build.
- By default, vendored mode sets `ARROW_DEPENDENCY_SOURCE` to `BUNDLED` (unless you explicitly provide a different value on the CMake command line).
- `ARROW_PARQUET` is enabled for vendored builds so Parquet libraries are built.
- The build defaults favor static libraries to avoid runtime .so dependencies:
  - `ARROW_BUILD_STATIC=ON` and `ARROW_BUILD_SHARED=OFF` (Arrow static libs are preferred)
  - `ARROW_DEPENDENCY_USE_SHARED=OFF` (prefer static third-party deps)
  - Snappy for Parquet is enabled by default (`ARROW_WITH_SNAPPY=ON`) and set to static (`ARROW_SNAPPY_USE_SHARED=OFF`).
- The top-level CMake creates convenient alias targets such as `Arrow::arrow` and `Parquet::parquet` when Arrow/Parquet library targets are available, and publishes an `ARROW_INCLUDE_DIRS` internal cache variable so downstream targets can find Arrow headers during compile.

System Boost interaction

- When vendoring Arrow and `ARROW_DEPENDENCY_SOURCE` is `BUNDLED`, the top-level build will quietly check whether a usable system Boost is present. If a complete system Boost is detected, the build will instruct Arrow to use the system Boost to avoid duplicate Boost targets (this is implemented by setting `Boost_SOURCE=SYSTEM` and `ARROW_BOOST_USE_SHARED=ON` in the cache).
- If you prefer to force Arrow to use system dependencies, set `-DARROW_DEPENDENCY_SOURCE=SYSTEM` on the CMake command line. If the system Arrow headers are not found in that mode, the top-level CMake may fall back to `BUNDLED` automatically so the vendored Arrow can build its dependencies.

Notes and troubleshooting

- Vendored builds can be heavier (longer) because Arrow and many of its third-party dependencies are built from source. Use `ccache` to speed repeated builds.
- If you see target-aliasing warnings about Arrow/Parquet targets, inspect the produced targets in the Arrow build subtree (`build-vendored/third_party_arrow_build/...`) and adjust `ARROW_*` options if necessary.


### Manual install steps for mpdecimal

Ubuntu 22.04 users can safely skip this section since `apt install libmpdec-dev` installs new version enough for Jogasaki. On Ubuntu 24.04 (and later), the apt command won't install mpdecimal package. Follow these steps in order to install mpdecmal in the custom location.

1. Download [mpdecimal-2.5.1.tar.gz](https://www.bytereef.org/software/mpdecimal/releases/mpdecimal-2.5.1.tar.gz) listed [here](https://www.bytereef.org/mpdecimal/download.html).
2. Untar the archive and move into the extracted directory.
```
$ cd mpdecimal-2.5.1
```
3. run `configure` specifying `--prefix` option with installation target directory.
```
./configure --prefix=<install directory>
```

4. build and install
```
make
make install
```

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
* `-DBUILD_TESTS=ON` - build test programs
* `-DBUILD_DOCUMENTS=ON` - build documents by doxygen
* `-DBUILD_EXAMPLES=ON` - build example applications
* `-DBUILD_STRICT=OFF` - don't treat compile warnings as build errors
* `-DINSTALL_EXAMPLES=ON` - install example applications (requires BUILD_EXAMPLES enabled)
* `-DSHARKSFIN_IMPLEMENTATION=<implementation name>` - switch sharksfin implementation. Available options are `memory` and `shirakami` (default: `shirakami`)
* `-DPERFORMANCE_TOOLS=ON` - enable performance tooling to measure engine performance
* `-DINSTALL_API_ONLY=ON` - configure build directory just to install public header files. Use when other components require jogasaki public headers.
* `-DENABLE_ALTIMETER=ON` - turn on altimeter logging.
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

### Code coverage

Run cmake with `-DENABLE_COVERAGE=ON` and run tests.
Dump the coverage information into html files with the following steps:

```sh
cd build
mkdir gcovr-html
GCOVR_COMMON_OPTION='-e ../third_party -e ../test -e ../examples -e ../mock -e (.+)?\.pb\.h$ -e (.+)?\.pb\.cc$'
gcovr  -r .. --html --html-details  ${GCOVR_COMMON_OPTION} -o gcovr-html/jogasaki-gcovr.html
```

Open gcovr-html/jogasaki-gcovr.html to see the coverage report.

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

