# Copyright 2018-2025 Project Tsurugi.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)

project(jogasaki
    VERSION 1.0.0
    DESCRIPTION "Jogasaki SQL Execution Engine"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)
option(BUILD_TESTS "Build test programs" OFF)
option(BUILD_EXAMPLES "Build example programs" OFF)
option(BUILD_DOCUMENTS "build documents" OFF)
option(BUILD_STRICT "build with option strictly determine of success" ON)
option(INSTALL_EXAMPLES "install examples" OFF)
option(BUILD_SHARED_LIBS "build shared libraries instead of static" ON)
option(ENABLE_GOOGLE_PERFTOOLS "Enable Google Perftools" OFF)
option(PERFORMANCE_TOOLS "Enable tooling to measure engine performance" OFF)
option(ENABLE_CACHE_ALIGN "enable optional cache align requirement" OFF)
option(TRACY_ENABLE "enable tracy profiler" OFF)
option(INSTALL_API_ONLY "configure cmake build dir. just to install public api files. No build runs. This is used to provide api files to components outside jogasaki" OFF)
option(LIKWID_ENABLE "enable likwid hardware counter profiling" OFF)
option(ENABLE_ALTIMETER "enable altimeter integration" OFF)

if (ENABLE_GOOGLE_PERFTOOLS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_GOOGLE_PERFTOOLS")
endif()

if(NOT DEFINED SHARKSFIN_IMPLEMENTATION)
    set(
            SHARKSFIN_IMPLEMENTATION "shirakami"
            CACHE STRING
            "sharksfin target name to link"
            FORCE
    )
endif()

set(tateyama_package "tateyama-${SHARKSFIN_IMPLEMENTATION}")
set(tateyama_engine "${tateyama_package}-engine")
set(ENGINE "engine")

if (NOT INSTALL_API_ONLY)
find_package(${tateyama_package} REQUIRED)
find_package(takatori REQUIRED)
find_package(yugawara REQUIRED)
find_package(mizugaki REQUIRED)
find_package(sharksfin REQUIRED)
find_package(limestone REQUIRED)
find_package(tsl-hopscotch-map 2.2 REQUIRED)
find_package(moodycamel QUIET)
find_package(Doxygen)
find_package(glog REQUIRED)
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost
        COMPONENTS filesystem thread system container
        REQUIRED
        )
find_package(TBB REQUIRED)
if(PERFORMANCE_TOOLS)
	find_package(performance-tools REQUIRED)
endif()
find_package(Protobuf REQUIRED)
find_package(mpdecpp REQUIRED)
find_package(likwid QUIET)
find_package(nlohmann_json REQUIRED)

if(ENABLE_ALTIMETER)
find_package(fmt REQUIRED)
find_package(altimeter REQUIRED)
endif()
find_package(gRPC REQUIRED)

# to suppress not-found warnings
set(re2Alt_FIND_QUIETLY TRUE)
set(c-aresAlt_FIND_QUIETLY TRUE)
set(lz4Alt_FIND_QUIETLY TRUE)
set(ThriftAlt_FIND_QUIETLY TRUE)
set(zstdAlt_FIND_QUIETLY TRUE)

# Arrow/Parquet dependencies
find_package(Arrow REQUIRED)

# Be lenient for newer Arrow versions
# find_package version range selector ... is available on cmake 3.19 or newer
set(SUPPORTED_ARROW_VERSION 14)
if(NOT DEFINED Arrow_VERSION_MAJOR OR Arrow_VERSION_MAJOR LESS ${SUPPORTED_ARROW_VERSION})
    message(FATAL_ERROR "Arrow_VERSION_MAJOR (${Arrow_VERSION_MAJOR}) is not defined or less than the supported version (${SUPPORTED_ARROW_VERSION}). Install Arrow version that is compatible with supported version.")
endif()

get_filename_component(MY_SEARCH_DIR ${Arrow_CONFIG} DIRECTORY)
find_package(Parquet REQUIRED HINTS ${MY_SEARCH_DIR})

add_subdirectory(third_party) # should be before enable_testing()
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)

if (BUILD_TESTS OR BUILD_EXAMPLES)
    enable_testing()
endif()

set(export_name "jogasaki-${SHARKSFIN_IMPLEMENTATION}")
set(package_name "jogasaki-${SHARKSFIN_IMPLEMENTATION}")

add_library(api INTERFACE)

target_include_directories(api
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/jogasaki>
)

target_link_libraries(api
        INTERFACE takatori
        INTERFACE yugawara
)

if (NOT INSTALL_API_ONLY)
add_subdirectory(src)
add_subdirectory(mock)
if(BUILD_TESTS)
    add_subdirectory(test)
endif()
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
if (BUILD_DOCUMENTS)
    add_subdirectory(doxygen)
endif()
endif()

configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Config.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        @ONLY
)

write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
        COMPATIBILITY SameMajorVersion
)

install_custom(api ${export_name})

install(
        FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake
        DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

install(
        EXPORT ${package_name}
        NAMESPACE ${package_name}-
        FILE ${package_name}-targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
        EXPORT_LINK_INTERFACE_LIBRARIES
)

