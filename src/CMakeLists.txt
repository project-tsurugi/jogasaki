if(NOT TARGET sharksfin-${SHARKSFIN_IMPLEMENTATION})
    message(FATAL_ERROR "sharksfin implementation \"sharksfin-${SHARKSFIN_IMPLEMENTATION}\" not found")
endif()

set(SqlProtoFiles
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/common.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/request.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/response.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/status.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/error.proto
        )

set(MetadataProtoFiles
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/metadata/common.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/metadata/storage.proto
        )

add_custom_command(
        OUTPUT ${SqlProtoFiles}
        COMMAND ${CMAKE_COMMAND}
        ARGS -E copy_if_different
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/proto/src/main/protos/jogasaki/proto/sql/common.proto"
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/proto/src/main/protos/jogasaki/proto/sql/request.proto"
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/proto/src/main/protos/jogasaki/proto/sql/response.proto"
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/proto/src/main/protos/jogasaki/proto/sql/status.proto"
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/proto/src/main/protos/jogasaki/proto/sql/error.proto"
        "${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql"
)

set(KvsserviceProtoFiles
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs/data.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs/request.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs/response.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs/transaction.proto
        )

add_custom_command(
        OUTPUT ${KvsserviceProtoFiles}
        COMMAND ${CMAKE_COMMAND}
        ARGS -E copy_if_different
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/kvs/src/main/proto/tateyama/proto/kvs/data.proto"
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/kvs/src/main/proto/tateyama/proto/kvs/request.proto"
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/kvs/src/main/proto/tateyama/proto/kvs/response.proto"
        "${CMAKE_SOURCE_DIR}/third_party/tsubakuro/modules/kvs/src/main/proto/tateyama/proto/kvs/transaction.proto"
        "${CMAKE_SOURCE_DIR}/src/tateyama/proto/kvs"
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
        "jogasaki/data/*.cpp"
        "jogasaki/error/*.cpp"
        "jogasaki/executor/*.cpp"
        "jogasaki/executor/batch/*.cpp"
        "jogasaki/executor/common/*.cpp"
        "jogasaki/executor/file/*.cpp"
        "jogasaki/executor/io/*.cpp"
        "jogasaki/executor/process/*.cpp"
        "jogasaki/executor/process/impl/*.cpp"
        "jogasaki/executor/process/impl/expression/*.cpp"
        "jogasaki/executor/process/impl/expression/details/*.cpp"
        "jogasaki/executor/process/impl/ops/*.cpp"
        "jogasaki/executor/process/impl/ops/details/*.cpp"
        "jogasaki/executor/process/abstract/*.cpp"
        "jogasaki/executor/exchange/*.cpp"
        "jogasaki/executor/exchange/aggregate/*.cpp"
        "jogasaki/executor/exchange/broadcast/*.cpp"
        "jogasaki/executor/exchange/deliver/*.cpp"
        "jogasaki/executor/exchange/forward/*.cpp"
        "jogasaki/executor/exchange/group/*.cpp"
        "jogasaki/executor/exchange/shuffle/*.cpp"
        "jogasaki/executor/function/*.cpp"
        "jogasaki/executor/function/incremental/*.cpp"
        "jogasaki/executor/sequence/*.cpp"
        "jogasaki/index/*.cpp"
        "jogasaki/kvs/*.cpp"
        "jogasaki/memory/*.cpp"
        "jogasaki/memory/details/*.cpp"
        "jogasaki/meta/*.cpp"
        "jogasaki/model/*.cpp"
        "jogasaki/plan/*.cpp"
        "jogasaki/recovery/*.cpp"
        "jogasaki/scheduler/*.cpp"
        "jogasaki/scheduler/details/*.cpp"
        "jogasaki/serializer/*.cpp"
        "jogasaki/utils/*.cpp"
)

set_source_files_properties(
        ${GENERATED_SQL_PROTO_SRCS}
        ${GENERATED_METADATA_PROTO_SRCS}
        ${GENERATED_KVSSERVICE_PROTO_SRCS}
        PROPERTIES
        GENERATED TRUE
        COMPILE_FLAGS "-Wno-unused-parameter -Wno-array-bounds"
)

add_library(${ENGINE}
        ${SOURCES}
        ${GENERATED_SQL_PROTO_SRCS}
        ${GENERATED_METADATA_PROTO_SRCS}
        ${GENERATED_KVSSERVICE_PROTO_SRCS}
)

add_dependencies(${ENGINE}
        build_protos
        )

set_target_properties(${ENGINE} PROPERTIES
        OUTPUT_NAME ${export_name}
        )

target_include_directories(${ENGINE}
        PRIVATE ${CMAKE_BINARY_DIR}/src
        PRIVATE .
)

# The following workaround is no longer necessary for cmake in 3.18 and later versions
if(PARQUET_VERSION VERSION_LESS 10.0.0)
  set(PARQUET_PREFIX "")
else()
  set(PARQUET_PREFIX "Parquet::")
endif()

target_link_libraries(${ENGINE}
        PUBLIC api
        PUBLIC ${tateyama_engine}
        PUBLIC takatori
        PUBLIC yugawara
        PRIVATE mizugaki
        PRIVATE shakujo-model
        PRIVATE shakujo-parser
        PRIVATE shakujo-analyzer
        PRIVATE sharksfin-${SHARKSFIN_IMPLEMENTATION}
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
        PRIVATE ${PARQUET_PREFIX}parquet_shared
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
        INTERFACE mizugaki
        INTERFACE shakujo-model
        INTERFACE shakujo-parser
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
        INTERFACE ${PARQUET_PREFIX}parquet_shared
        INTERFACE mpdecpp
        )

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
