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

# Ensure a default build type so subprojects that call string(TOLOWER ${CMAKE_BUILD_TYPE} ...)
# do not see an empty value which causes CMake errors when used as a subproject.
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
endif()

# Ensure Arrow SIMD level has a sane default so Arrow's CMake conditionals parse
# correctly when Arrow is added as a subdirectory.
if(NOT DEFINED ARROW_SIMD_LEVEL)
    set(ARROW_SIMD_LEVEL "NONE" CACHE STRING "Preferred Arrow SIMD level")
endif()

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


# If vendoring Arrow and caller requested SYSTEM, sanity-check that the Arrow
# development headers actually exist in the system include path before we go
# ahead and find system Boost. If the headers are missing, switch to BUNDLED
# so Arrow will build its bundled third-party dependencies instead.
# Decide whether to find system Boost now. When vendoring Arrow with
# ARROW_DEPENDENCY_SOURCE other than SYSTEM, Arrow may build a bundled Boost
# and creating system Boost:: targets here causes alias/duplicate target
# errors. So skip finding Boost in that case.
set(_find_system_boost TRUE)
if(DEFINED USE_VENDORED_ARROW AND USE_VENDORED_ARROW)
    if(NOT DEFINED ARROW_DEPENDENCY_SOURCE OR NOT ARROW_DEPENDENCY_SOURCE STREQUAL "SYSTEM")
        set(_find_system_boost FALSE)
    endif()
endif()

if(_find_system_boost)
    find_package(Boost
        COMPONENTS filesystem thread system container
        REQUIRED
    )
endif()
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

# to suppress not-found warnings
set(re2Alt_FIND_QUIETLY TRUE)
set(c-aresAlt_FIND_QUIETLY TRUE)
set(lz4Alt_FIND_QUIETLY TRUE)
set(ThriftAlt_FIND_QUIETLY TRUE)
set(zstdAlt_FIND_QUIETLY TRUE)

# Arrow/Parquet dependencies

# Option to use vendored Arrow/Parquet under third_party (add_subdirectory)
option(USE_VENDORED_ARROW "Use Arrow/Parquet under third_party instead of system-installed packages" OFF)

if(USE_VENDORED_ARROW)
    set(ARROW_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/arrow")
    if(NOT EXISTS "${ARROW_SOURCE_DIR}/cpp/CMakeLists.txt")
        message(FATAL_ERROR "Vendored Arrow not found under ${ARROW_SOURCE_DIR} (expected cpp/CMakeLists.txt)")
    endif()

    # Allow caller to override Arrow build options; default to building Arrow's
    # bundled third-party dependencies so vendored mode works even when system
    # libarrow-dev/libparquet-dev packages are not installed.
    if(NOT DEFINED ARROW_DEPENDENCY_SOURCE)
        set(ARROW_DEPENDENCY_SOURCE "BUNDLED" CACHE STRING "Arrow dependency source (AUTO|BUNDLED|SYSTEM|CONDA|VCPKG|BREW)")
    endif()

    # Ensure Parquet is enabled when vendoring
    if(NOT DEFINED ARROW_PARQUET)
        set(ARROW_PARQUET ON CACHE BOOL "Build Parquet with Arrow" FORCE)
    endif()

    # If we're vendoring Arrow and using BUNDLED for most deps, but the
    # system already provides Boost, prefer using the system Boost to avoid
    # duplicated Boost::target name collisions when Arrow attempts to build
    # its bundled Boost. Use a quiet find so we don't spam output.
    if(DEFINED ARROW_DEPENDENCY_SOURCE AND ARROW_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
        find_package(Boost QUIET COMPONENTS filesystem thread system container)
        if(Boost_FOUND)
            message(STATUS "System Boost detected; instructing Arrow to use SYSTEM Boost to avoid conflicts.")
            set(Boost_SOURCE "SYSTEM" CACHE STRING "Boost dependency source" FORCE)
            # Mirror Arrow's boost preference so Arrow's ThirdpartyToolchain will pick system Boost
            set(ARROW_BOOST_USE_SHARED ON CACHE BOOL "Use shared Boost libs" FORCE)
        endif()
    endif()

    # If caller requested SYSTEM dependencies for vendored Arrow, do a quick
    # sanity check that Arrow development headers are available on the system
    # include path. If they are not present, automatically fall back to
    # BUNDLED so the vendored Arrow will build its bundled third-party
    # dependencies and provide the headers/libraries required by jogasaki.
    if(DEFINED ARROW_DEPENDENCY_SOURCE AND ARROW_DEPENDENCY_SOURCE STREQUAL "SYSTEM")
        find_path(ARROW_SYSTEM_INCLUDE_DIR "arrow/io/file.h")
        if(NOT ARROW_SYSTEM_INCLUDE_DIR)
            message(WARNING "ARROW_DEPENDENCY_SOURCE=SYSTEM but 'arrow/io/file.h' was not found in system include paths. Falling back to ARROW_DEPENDENCY_SOURCE=BUNDLED for vendored Arrow.")
            set(ARROW_DEPENDENCY_SOURCE "BUNDLED" CACHE STRING "Arrow dependency source (AUTO|BUNDLED|SYSTEM|CONDA|VCPKG|BREW)" FORCE)
        else()
            message(STATUS "Found system Arrow headers: ${ARROW_SYSTEM_INCLUDE_DIR}")
        endif()
    endif()

    # Add Arrow cpp as a subdirectory. Build output placed in separate build subtree.
    # When Arrow is added as a subdirectory, force it to define its options
    # so that it behaves like a top-level build (creates library targets and
    # installs/exports). This avoids missing-export errors during CMake
    # generate step.
    set(ARROW_DEFINE_OPTIONS ON CACHE BOOL "Define Arrow options when used as subproject" FORCE)
    # When vendoring, default to building static Arrow libraries so the
    # top-level project can link them into jogasaki and avoid runtime
    # dependencies on libarrow.so. Allow callers to override, but make the
    # static-default the safe option for vendored builds.
    # Note: We prefer Arrow's static libraries when vendoring. Avoid forcing
    # the global BUILD_SHARED_LIBS cache entry unconditionally which can have
    # surprising effects for other subprojects; only set a default value so
    # callers can still override via the cache or CLI.
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries instead of static")
    set(ARROW_BUILD_SHARED OFF CACHE BOOL "Build Arrow shared libraries" FORCE)
    set(ARROW_BUILD_STATIC ON CACHE BOOL "Build Arrow static libraries" FORCE)
    # Prefer static third-party dependencies for Arrow when vendoring.
    set(ARROW_DEPENDENCY_USE_SHARED OFF CACHE BOOL "Use shared thirdparty deps for Arrow" FORCE)
    # Also prefer system/static Boost if available to avoid shared dependency surprises.
    # Only set a default here; the earlier detection branch that flips
    # ARROW_BOOST_USE_SHARED to ON when a system Boost is detected will
    # intentionally override this default.
    if(NOT DEFINED ARROW_BOOST_USE_SHARED)
        set(ARROW_BOOST_USE_SHARED OFF CACHE BOOL "Use shared Boost libs" )
    endif()
    # Enable snappy support for Parquet when vendoring Arrow. Prefer static
    # snappy to avoid introducing additional shared library dependencies.
    set(ARROW_WITH_SNAPPY ON CACHE BOOL "Enable Snappy compression support in Arrow/Parquet" FORCE)
    set(ARROW_SNAPPY_USE_SHARED OFF CACHE BOOL "Use shared Snappy library for Arrow" FORCE)
    add_subdirectory("${ARROW_SOURCE_DIR}/cpp" "${CMAKE_CURRENT_BINARY_DIR}/third_party_arrow_build")

    # Create commonly expected alias targets so existing target_link_libraries calls work.
    # Arrow target candidates (may vary by Arrow version/config)
    if(TARGET arrow_shared)
        add_library(Arrow::arrow ALIAS arrow_shared)
    elseif(TARGET arrow_static)
        add_library(Arrow::arrow ALIAS arrow_static)
    elseif(TARGET arrow)
        # Only create an alias if 'arrow' is an actual library target.
        get_target_property(_arrow_type arrow TYPE)
        if(_arrow_type STREQUAL "SHARED_LIBRARY" OR _arrow_type STREQUAL "STATIC_LIBRARY")
            add_library(Arrow::arrow ALIAS arrow)
        else()
            message(WARNING "Found target 'arrow' but it is not a library (TYPE=${_arrow_type}); skipping Arrow::arrow alias")
        endif()
    else()
        message(WARNING "Could not find a known Arrow target (arrow_shared/arrow_static/arrow). You may need to adjust aliasing.")
    endif()

    # If Arrow did not create a canonical "Arrow::arrow" target, try to
    # synthesize one from the built library files so downstream code can
    # reliably link to Arrow via a target. This avoids passing around
    # include-directory cache variables like ARROW_INCLUDE_DIRS.
    if(NOT TARGET Arrow::arrow)
        # Look for a built static archive under the Arrow build subtree.
        file(GLOB_RECURSE _arrow_static_libs
            "${CMAKE_CURRENT_BINARY_DIR}/third_party_arrow_build/*/libarrow*.a"
            "${CMAKE_CURRENT_BINARY_DIR}/third_party_arrow_build/lib*/libarrow*.a"
        )
        if(_arrow_static_libs)
            list(GET _arrow_static_libs 0 _arrow_static_lib)
            add_library(Arrow::arrow STATIC IMPORTED GLOBAL)
            set_target_properties(Arrow::arrow PROPERTIES
                IMPORTED_LOCATION "${_arrow_static_lib}"
            )
            # Provide a reasonable set of interface include directories so
            # consumers that link Arrow::arrow get headers automatically.
            if(DEFINED ARROW_SOURCE_DIR)
                set(_arrow_src_include "${ARROW_SOURCE_DIR}/cpp/src")
            else()
                set(_arrow_src_include "${CMAKE_CURRENT_SOURCE_DIR}/third_party/arrow/cpp/src")
            endif()
            set(_arrow_build_include "${CMAKE_CURRENT_BINARY_DIR}/third_party_arrow_build/src")
            set(_arrow_build_parquet_include "${CMAKE_CURRENT_BINARY_DIR}/third_party_arrow_build/src/parquet")
            set_target_properties(Arrow::arrow PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${_arrow_src_include};${_arrow_build_include};${_arrow_build_parquet_include}"
            )
            message(STATUS "Created imported Arrow::arrow target from ${_arrow_static_lib}")
        endif()
    endif()

    # Parquet target aliasing
    if(TARGET parquet_shared)
        add_library(Parquet::parquet ALIAS parquet_shared)
    elseif(TARGET parquet_static)
        add_library(Parquet::parquet ALIAS parquet_static)
    elseif(TARGET parquet)
        # Only create an alias if 'parquet' is an actual library target.
        get_target_property(_parquet_type parquet TYPE)
        if(_parquet_type STREQUAL "SHARED_LIBRARY" OR _parquet_type STREQUAL "STATIC_LIBRARY")
            add_library(Parquet::parquet ALIAS parquet)
        else()
            message(WARNING "Found target 'parquet' but it is not a library (TYPE=${_parquet_type}); skipping Parquet::parquet alias")
        endif()
    else()
        message(WARNING "Could not find a known Parquet target. If your build fails, inspect parquet target names.")
    endif()
    # When Arrow is vendored, extract the Arrow target's interface include
    # directories and publish them so downstream targets (for example the
    # `engine` target in src/) can consume Arrow headers at compile time.
    # This is necessary because Arrow exports its include dirs via imported
    # targets created in its generated export files; exposing them here makes
    # them easily available to the rest of the build.
    set(_arrow_interface_includes "")
    if(TARGET Arrow::arrow_shared)
        get_target_property(_arrow_interface_includes Arrow::arrow_shared INTERFACE_INCLUDE_DIRECTORIES)
    elseif(TARGET Arrow::arrow_static)
        get_target_property(_arrow_interface_includes Arrow::arrow_static INTERFACE_INCLUDE_DIRECTORIES)
    elseif(TARGET arrow_shared)
        get_target_property(_arrow_interface_includes arrow_shared INTERFACE_INCLUDE_DIRECTORIES)
    elseif(TARGET arrow)
        get_target_property(_arrow_interface_includes arrow INTERFACE_INCLUDE_DIRECTORIES)
    endif()
    if(_arrow_interface_includes)
        # Make available to subprojects (src/) in a safe, internal cache variable.
        # Use FORCE to replace any stale cache value that might contain
        # unresolved placeholders from earlier configure runs.
        set(ARROW_INCLUDE_DIRS "${_arrow_interface_includes}" CACHE INTERNAL "Arrow include directories for vendored Arrow" FORCE)
    else()
        # As a fallback, expose Arrow's source include directory so headers
        # such as <arrow/io/file.h> are found when Arrow's exported target
        # properties are not available or are not yet populated.
        if(DEFINED ARROW_SOURCE_DIR)
            set(_arrow_source_include "${ARROW_SOURCE_DIR}/cpp/src")
            # Also add Arrow's build-tree generated include directories where
            # files like parquet_version.h are created during the Arrow build.
            set(_arrow_build_include "${CMAKE_CURRENT_BINARY_DIR}/third_party_arrow_build/src")
            set(_arrow_build_parquet_include "${CMAKE_CURRENT_BINARY_DIR}/third_party_arrow_build/src/parquet")
            # Build a clean list without referencing the possibly undefined
            # _arrow_interface_includes variable.
            set(_arrow_includes_list "${_arrow_source_include};${_arrow_build_include};${_arrow_build_parquet_include}")
            set(ARROW_INCLUDE_DIRS "${_arrow_includes_list}" CACHE INTERNAL "Arrow include directories for vendored Arrow (fallback)" FORCE)
        endif()
    endif()
else()
    find_package(Arrow REQUIRED)

    # Be lenient for newer Arrow versions
    # find_package version range selector ... is available on cmake 3.19 or newer
    set(SUPPORTED_ARROW_VERSION 14)
    if(NOT DEFINED Arrow_VERSION_MAJOR OR Arrow_VERSION_MAJOR LESS ${SUPPORTED_ARROW_VERSION})
        message(FATAL_ERROR "Arrow_VERSION_MAJOR (${Arrow_VERSION_MAJOR}) is not defined or less than the supported version (${SUPPORTED_ARROW_VERSION}). Install Arrow version that is compatible with supported version.")
    endif()

    get_filename_component(MY_SEARCH_DIR ${Arrow_CONFIG} DIRECTORY)
    find_package(Parquet REQUIRED HINTS ${MY_SEARCH_DIR})
endif()

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

