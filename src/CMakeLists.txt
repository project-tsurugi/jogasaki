if(NOT TARGET sharksfin-${SHARKSFIN_IMPLEMENTATION})
    message(FATAL_ERROR "sharksfin implementation \"sharksfin-${SHARKSFIN_IMPLEMENTATION}\" not found")
endif()

file(GLOB SOURCES
        "jogasaki/*.cpp"
        "jogasaki/accessor/*.cpp"
        "jogasaki/api/*.cpp"
        "jogasaki/api/impl/*.cpp"
        "jogasaki/api/service/*.cpp"
        "jogasaki/api/resource/*.cpp"
        "jogasaki/data/*.cpp"
        "jogasaki/executor/*.cpp"
        "jogasaki/executor/common/*.cpp"
        "jogasaki/executor/file/*.cpp"
        "jogasaki/executor/io/*.cpp"
        "jogasaki/executor/process/*.cpp"
        "jogasaki/executor/process/impl/*.cpp"
        "jogasaki/executor/process/impl/expression/*.cpp"
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
        "jogasaki/logship/*.cpp"
        "jogasaki/memory/*.cpp"
        "jogasaki/memory/details/*.cpp"
        "jogasaki/meta/*.cpp"
        "jogasaki/model/*.cpp"
        "jogasaki/plan/*.cpp"
        "jogasaki/scheduler/*.cpp"
        "jogasaki/scheduler/details/*.cpp"
        "jogasaki/kvs/*.cpp"
        "jogasaki/utils/*.cpp"
        "jogasaki/endpoint/*.cpp"
)


set_source_files_properties(
        ${GENERATED_PROTO_SRCS}
        PROPERTIES
        GENERATED TRUE
        COMPILE_FLAGS -Wno-unused-parameter
)

add_library(${ENGINE}
        ${SOURCES}
        ${GENERATED_PROTO_SRCS}
)

add_dependencies(${ENGINE}
        build_protos
        )

set_target_properties(${ENGINE} PROPERTIES
        OUTPUT_NAME ${export_name}
        )

target_include_directories(${ENGINE}
        PRIVATE ${CMAKE_BINARY_DIR}/protos
        PRIVATE .
)

target_link_libraries(${ENGINE}
        PUBLIC api
        PUBLIC ${tateyama_engine}
        PUBLIC takatori
        PUBLIC yugawara
        PRIVATE mizugaki
        PRIVATE shakujo-model
        PRIVATE shakujo-parser
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
        PRIVATE parquet_shared
        )

if(LOGSHIP)
    target_link_libraries(${ENGINE} PRIVATE hayatsuki-engine)
endif()

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
        INTERFACE ${CMAKE_BINARY_DIR}/protos
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
        INTERFACE parquet_shared
        )

if(LOGSHIP)
    target_link_libraries(jogasaki-impl INTERFACE hayatsuki-engine)
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
