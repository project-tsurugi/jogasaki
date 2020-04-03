# Jogasaki - A SQL execution engine 

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* and see *Dockerfile* section

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-filesystem-dev libboost-system-dev libboost-container-dev libboost-thread-dev libgoogle-glog-dev libgflags-dev doxygen libtbb-dev
```


optional packages:

* `doxygen`
* `graphviz`
* `clang-tidy-8`

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

available options:
* `-DCMAKE_INSTALL_PREFIX=<installation directory>` - change install location
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequisite installation directory
* `-DBUILD_SHARED_LIBS=OFF` - create static libraries instead of shared libraries
* `-DBUILD_TESTS=OFF` - don't build test programs
* `-DBUILD_DOCUMENTS=OFF` - don't build documents by doxygen
* `-DFORCE_INSTALL_RPATH=ON` - automatically configure `INSTALL_RPATH` for non-default library paths

### install 

```sh
ninja install
```

### run tests

```sh
ctest -V
```

### generate documents

```sh
ninja doxygen
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

