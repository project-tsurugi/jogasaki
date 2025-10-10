if(NOT TARGET sharksfin-${SHARKSFIN_IMPLEMENTATION})
    message(FATAL_ERROR "sharksfin implementation \"sharksfin-${SHARKSFIN_IMPLEMENTATION}\" not found")
endif()

set(SqlProtoFiles
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/common.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/request.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/response.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/error.proto
        )

set(MetadataProtoFiles
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/metadata/common.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/metadata/storage.proto
        )

set(KvsserviceProtoFiles
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs/data.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs/request.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs/response.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs/transaction.proto
        )

# By default, PROTOBUF_GENERATE_CPP generates file path for .pb.cc as if they are in the same directory.
# Work-around this with PROTOBUF_GENERATE_CPP_APPEND_PATH
set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF)

PROTOBUF_GENERATE_CPP(SqlProtobufSources SqlProtobufHeaders ${SqlProtoFiles})
PROTOBUF_GENERATE_CPP(MetadataProtobufSources MetadataProtobufHeaders ${MetadataProtoFiles})
PROTOBUF_GENERATE_CPP(KvsserviceProtobufSources KvsserviceProtobufHeaders ${KvsserviceProtoFiles})

set(GENERATED_SQL_PROTO_SRCS ${SqlProtobufSources})
set(GENERATED_METADATA_PROTO_SRCS ${MetadataProtobufSources})

set(GENERATED_KVSSERVICE_PROTO_SRCS ${KvsserviceProtobufSources})

add_custom_target(build_protos
        DEPENDS
        ${SqlProtobufSources}
        ${SqlProtoFiles}
        ${MetadataProtobufSources}
        ${SqlProtoFiles}
        ${KvsserviceProtoFiles}
        ${KvsserviceProtobufSources}
        )

install(DIRECTORY
        ${CMAKE_BINARY_DIR}/src/jogasaki
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/jogasaki
        FILES_MATCHING
        PATTERN "proto/sql/*.pb.h"
        )

install(DIRECTORY
        ${CMAKE_BINARY_DIR}/src/tateyama
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tateyama
        FILES_MATCHING
        PATTERN "proto/kvs/*.pb.h"
        )

file(GLOB SOURCES
        "jogasaki/*.cpp"
        "jogasaki/accessor/*.cpp"
        "jogasaki/api/*.cpp"
        "jogasaki/api/impl/*.cpp"
        "jogasaki/api/kvsservice/*.cpp"
        "jogasaki/api/kvsservice/impl/*.cpp"
        "jogasaki/api/service/*.cpp"
        "jogasaki/api/resource/*.cpp"
        "jogasaki/auth/*.cpp"
        "jogasaki/data/*.cpp"
        "jogasaki/datastore/*.cpp"
        "jogasaki/error/*.cpp"
        "jogasaki/external_log/*.cpp"
        "jogasaki/executor/*.cpp"
        "jogasaki/executor/batch/*.cpp"
        "jogasaki/executor/common/*.cpp"
        "jogasaki/executor/conv/*.cpp"
        "jogasaki/executor/dto/*.cpp"
        "jogasaki/executor/file/*.cpp"
        "jogasaki/executor/io/*.cpp"
        "jogasaki/executor/process/*.cpp"
        "jogasaki/executor/process/impl/*.cpp"
        "jogasaki/executor/expr/*.cpp"
        "jogasaki/executor/expr/details/*.cpp"
        "jogasaki/executor/process/impl/ops/*.cpp"
        "jogasaki/executor/process/impl/ops/details/*.cpp"
        "jogasaki/executor/process/abstract/*.cpp"
        "jogasaki/executor/exchange/*.cpp"
        "jogasaki/executor/exchange/aggregate/*.cpp"
        "jogasaki/executor/exchange/forward/*.cpp"
        "jogasaki/executor/exchange/group/*.cpp"
        "jogasaki/executor/exchange/shuffle/*.cpp"
        "jogasaki/executor/function/*.cpp"
        "jogasaki/executor/function/incremental/*.cpp"
        "jogasaki/executor/sequence/*.cpp"
        "jogasaki/executor/wrt/*.cpp"
        "jogasaki/index/*.cpp"
        "jogasaki/kvs/*.cpp"
        "jogasaki/memory/*.cpp"
        "jogasaki/memory/details/*.cpp"
        "jogasaki/meta/*.cpp"
        "jogasaki/meta/impl/*.cpp"
        "jogasaki/model/*.cpp"
        "jogasaki/plan/*.cpp"
        "jogasaki/recovery/*.cpp"
        "jogasaki/scheduler/*.cpp"
        "jogasaki/scheduler/details/*.cpp"
        "jogasaki/serializer/*.cpp"
        "jogasaki/storage/*.cpp"
        "jogasaki/utils/*.cpp"
        "jogasaki/dist/*.cpp"
)

if(ENABLE_ALTIMETER)
file(GLOB SRC_ALTIMETER_ENABLED
        "jogasaki/external_log/details/*.cpp"
)
list (APPEND SOURCES ${SRC_ALTIMETER_ENABLED})
endif()

set_source_files_properties(
        ${GENERATED_SQL_PROTO_SRCS}
        ${GENERATED_METADATA_PROTO_SRCS}
        ${GENERATED_KVSSERVICE_PROTO_SRCS}
        PROPERTIES
        GENERATED TRUE
        COMPILE_FLAGS "-Wno-unused-parameter -Wno-array-bounds"
)

add_library(${ENGINE} SHARED
        ${SOURCES}
        ${GENERATED_SQL_PROTO_SRCS}
        ${GENERATED_METADATA_PROTO_SRCS}
        ${GENERATED_KVSSERVICE_PROTO_SRCS}
)

# Ensure shared library is compiled with position independent code so it can
# be linked against static archives (for example, vendored static Arrow
# libraries). This explicitly sets PIC for the target independent of the
# global BUILD_SHARED_LIBS value.
set_target_properties(${ENGINE} PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_dependencies(${ENGINE}
        build_protos
        )

set_target_properties(${ENGINE}
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN"
                OUTPUT_NAME ${export_name}
)

target_include_directories(${ENGINE}
        PRIVATE ${CMAKE_BINARY_DIR}/src
        PRIVATE .
)

# Prefer target-based Arrow usage when possible. If the Arrow CMake
# project (vendored or system) provides a proper `Arrow::arrow` target
# then link the engine against it so include directories and link
# dependencies are propagated via target properties. Otherwise fall back
# to the legacy `ARROW_INCLUDE_DIRS` cache variable which publishes
# Arrow's include directories when Arrow is vendored via add_subdirectory.
if(TARGET Arrow::arrow)
    target_link_libraries(${ENGINE}
            PRIVATE Arrow::arrow
    )
        # Some Arrow builds may not populate target INTERFACE_INCLUDE_DIRECTORIES
        # reliably when used as a subproject. If a fallback ARROW_INCLUDE_DIRS
        # cache variable was published (see top-level CMakeLists), also add
        # those directories as SYSTEM include paths so headers like
        # <arrow/io/file.h> are found during compilation.
        if(DEFINED ARROW_INCLUDE_DIRS)
                target_include_directories(${ENGINE}
                                SYSTEM PRIVATE ${ARROW_INCLUDE_DIRS}
                )
        endif()
else()
    if(DEFINED ARROW_INCLUDE_DIRS)
        # Treat Arrow include directories as system headers to avoid
        # promoting warnings from vendored Arrow headers into -Werror
        # failures for our project sources. The SYSTEM keyword must appear
        # before the scope (PRIVATE) per CMake's signature.
        target_include_directories(${ENGINE}
                SYSTEM PRIVATE ${ARROW_INCLUDE_DIRS}
        )
    endif()
endif()

# The following workaround is no longer necessary for cmake in 3.18 and later versions
if(PARQUET_VERSION VERSION_LESS 10.0.0)
  set(PARQUET_PREFIX "")
else()
  set(PARQUET_PREFIX "Parquet::")
endif()

# Determine a safe, linkable parquet target. Prefer the canonical Parquet::parquet
# alias if available (it maps to parquet_static or parquet_shared). Otherwise
# fall back to explicit static/shared targets created by the Arrow build.
if(TARGET Parquet::parquet)
        set(_parquet_link_target Parquet::parquet)
elseif(TARGET parquet_static)
        set(_parquet_link_target parquet_static)
elseif(TARGET parquet_shared)
        set(_parquet_link_target parquet_shared)
elseif(TARGET parquet)
        # Some Arrow versions define a 'parquet' utility target; avoid linking to
        # utility targets if possible, but fall back to it as a last resort.
        set(_parquet_link_target parquet)
else()
        # Use the prefix expansion as a final fallback. This may resolve to
        # Parquet::parquet when find_package(Parquet) was used instead of vendoring.
        set(_parquet_link_target "${PARQUET_PREFIX}parquet")
endif()

target_link_libraries(${ENGINE}
        PUBLIC api
        PUBLIC ${tateyama_engine}
        PUBLIC takatori
        PUBLIC yugawara
        PRIVATE mizugaki
        PRIVATE sharksfin-${SHARKSFIN_IMPLEMENTATION}
        PRIVATE limestone
        PRIVATE tbb
        PRIVATE numa
        PRIVATE tsl::hopscotch_map
        PRIVATE Boost::boost
        PRIVATE Boost::filesystem
        PRIVATE Boost::thread
        PRIVATE Boost::container
        PRIVATE glog::glog
        PRIVATE atomic
        PRIVATE protobuf::libprotobuf
        # Link against the canonical Parquet target. The top-level CMakeLists
        # creates `Parquet::parquet` as an alias to either the static or
        # shared parquet target depending on how Arrow was configured. Using
        # the alias lets us avoid hard-coding target names and ensures the
        # static variant is used when available.
        PRIVATE ${_parquet_link_target}
        PRIVATE mpdecpp
        )

if(LIKWID_ENABLE)
    target_link_libraries(${ENGINE}
            PRIVATE likwid::likwid
            )
endif()

if(MC_QUEUE)
    target_compile_definitions(${ENGINE} PUBLIC MC_QUEUE)
endif()

if (PERFORMANCE_TOOLS)
    target_link_libraries(${ENGINE}
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(${ENGINE} PUBLIC PERFORMANCE_TOOLS)
endif()

if(ENABLE_ALTIMETER)
        target_link_libraries(${ENGINE}
                PRIVATE altimeter
        )
endif()

# Boost.Thread doesn't seem to allow multiple versions to coexist.
# This version definition should be shared with caller at least.
target_compile_definitions(${ENGINE} PUBLIC BOOST_THREAD_VERSION=4)

set_compile_options(${ENGINE})

install_custom(${ENGINE} ${export_name})

# for tests
add_library(jogasaki-impl INTERFACE)

target_include_directories(jogasaki-impl
        INTERFACE ${CMAKE_BINARY_DIR}/src
        INTERFACE .
        )

target_link_libraries(jogasaki-impl
        INTERFACE ${ENGINE}
        INTERFACE ${tateyama_engine}
        INTERFACE takatori
        INTERFACE yugawara
        INTERFACE sharksfin-${SHARKSFIN_IMPLEMENTATION}
        INTERFACE tbb
        INTERFACE numa
        INTERFACE tsl::hopscotch_map
        INTERFACE Boost::boost
        INTERFACE Boost::filesystem
        INTERFACE Boost::thread
        INTERFACE Boost::container
        INTERFACE glog::glog
        INTERFACE protobuf::libprotobuf
        # Use the same parquet link target resolution as the engine target so
        # tests and consumers don't hard-code parquet_shared which may not be
        # available when Arrow is vendored and built static. The variable
        # `_parquet_link_target` is resolved earlier in this file.
        INTERFACE ${_parquet_link_target}
        INTERFACE mpdecpp
        )

if(TARGET Arrow::arrow)
    # Export Arrow target transitively through the test/impl interface so
    # consumers (tests, examples) don't need to manually add include dirs.
    target_link_libraries(jogasaki-impl
            INTERFACE Arrow::arrow
    )
endif()

if(MC_QUEUE)
    target_compile_definitions(jogasaki-impl INTERFACE MC_QUEUE)
endif()

if(LIKWID_ENABLE)
    target_link_libraries(jogasaki-impl
            INTERFACE likwid::likwid
            )
endif()

if (PERFORMANCE_TOOLS)
    target_link_libraries(jogasaki-impl
            INTERFACE performance-tools::api
            INTERFACE performance-tools::lib
            )
    target_compile_definitions(jogasaki-impl INTERFACE PERFORMANCE_TOOLS)
endif()
