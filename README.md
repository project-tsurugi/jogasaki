# Jogasaki - A SQL execution engine 

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* access to installed dependent modules: 
  * sharksfin
  * tateyama
  * takatori
  * yugawara
  * mizugaki
  * shakujo (until dependency is removed)
  * mpdecimal 2.5.1 (see `Install steps for mpdecimal` section below)
  * tsubakuro (for proto definition)
  * performance-tools (optional)
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:20.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-filesystem-dev libboost-system-dev libboost-container-dev libboost-thread-dev libboost-stacktrace-dev libgoogle-glog-dev libgflags-dev doxygen libtbb-dev libnuma-dev protobuf-compiler protobuf-c-compiler libprotobuf-dev libmsgpack-dev uuid-dev libicu-dev pkg-config flex bison libparquet-dev=9.0.0-1 libparquet-glib-dev=9.0.0-1
```
(see "Additional file installation for Apache Parquet" below if installing `libparquet-dev`, `libparquet-glib-dev` fails)

optional packages:

* `doxygen`
* `graphviz`
* `clang-tidy-8`
* [`linenoise-ng`](https://github.com/arangodb/linenoise-ng.git)

## Additional file installation for Apache Parquet

Jogasaki requires Apache Parquet package versioned as `9.0.0.-1` (Official release stays to this version for stability. Jogasaki may be built and run with later versions, but it's for development/testing purpose only, not for production.) 

Installing Apache Paquet packages `libparquet-dev`, `libparquet-glib-dev` requires additional files installation. 
If installing these packages from `apt install` command fails, issue below commands to install required files.

```
sudo apt install -y -V lsb-release
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libparquet-dev=9.0.0-1 libparquet-glib-dev=9.0.0-1 libarrow-dev=9.0.0-1 libarrow-glib-dev=9.0.0-1 gir1.2-parquet-1.0=9.0.0-1 gir1.2-arrow-1.0=9.0.0-1
```

(You can see [here](https://arrow.apache.org/install/) for full instruction. )

## Install steps for mpdecimal

The apt command installs slightly old mpdecimal package (2.4) while jogasaki requires newer version(2.5 or later.) 
Follow these steps in order to install mpdecmal in the custom location.

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
* `-DBUILD_TESTS=OFF` - don't build test programs
* `-DBUILD_DOCUMENTS=OFF` - don't build documents by doxygen
* `-DINSTALL_EXAMPLES=ON` - install example applications
* `-DFORCE_INSTALL_RPATH=ON` - automatically configure `INSTALL_RPATH` for non-default library paths
* `-DSHARKSFIN_IMPLEMENTATION=<implementation name>` - switch sharksfin implementation. Available options are `memory` and `shirakami` (default: `memory`)
* `-DPERFORMANCE_TOOLS=ON` - enable performance tooling to measure engine performance
* `-DINSTALL_API_ONLY=ON` - configure build directory just to install public header files. Use when other components require jogasaki public headers.
* `-DLOGSHIP=ON` - enable logshipping integration with hayatsuki. Require installing hayatsuki beforehand.
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

### Logshipping integration with hayatsuki

Integrating hayatsuki enable sending jogasaki data to hayatsuki collector for log shipping. To enable log shipping, you need to follow these steps:

0. build and install [hayatsuki](https://github.com/project-tsurugi/hayatsuki). See hayatsuki [instruction](https://github.com/project-tsurugi/hayatsuki/tree/master/cpp) for detailed steps.
1. build jogasaki with `-DLOGSHIP=ON` build option.
2. Setup RabbitMQ service in order for hayatsuki to store shipping data. See hayatsuki [instruction](https://github.com/project-tsurugi/hayatsuki/tree/master/cpp) for detailed steps. Running RabbitMQ on docker is the handy way. For example: 
```
$ docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

After starting the container, attach to it to execute bash:
```
$ docker exec -it <container id> bash
```

In the console start as root, issue following commands to configure RabbitMQ instance for hayatsuki:
```
# rabbitmqctl add_user hayatsuki_user hayatsuki_pass
# rabbitmqctl delete_vhost /
# rabbitmqctl add_vhost /
# rabbitmqctl set_permissions -p / hayatsuki_user ".*" ".*" ".*"
# rabbitmqctl set_user_tags hayatsuki_user administrator
# rabbitmq-plugins enable rabbitmq_management
# rabbitmqadmin -u hayatsuki_user -p hayatsuki_pass declare exchange --vhost=/ name=hayatsuki.fanout type=fanout durable=true auto_delete=false internal=false
# rabbitmqadmin -u hayatsuki_user -p hayatsuki_pass declare queue --vhost=/ name=hayatsuki durable=true auto_delete=false
# rabbitmqadmin -u hayatsuki_user -p hayatsuki_pass declare binding --vhost=/  source=hayatsuki.fanout destination_type=queue destination=hayatsuki routing_key=
```

3. Run test/logship/logship_test.cpp to verify callback from sharksfin and send records to hayatsuki.

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

