file(GLOB SOURCES
        "jogasaki/executor/exchange/mock/aggregate/*.cpp"
        "jogasaki/executor/process/mock/*.cpp"
        "jogasaki/mock/*.cpp"
        "tateyama/api/endpoint/mock/*.cpp"
        "tateyama/api/server/mock/*.cpp"
        "jogasaki/utils/*.cpp"
)

set_source_files_properties(
        ${GENERATED_SQL_PROTO_SRCS}
        PROPERTIES
        GENERATED TRUE
        COMPILE_FLAGS -Wno-unused-parameter
)

add_library(common
        ${SOURCES}
        ${GENERATED_SQL_PROTO_SRCS}
)

add_dependencies(common
        build_protos
        )

target_include_directories(common
        PUBLIC ${CMAKE_BINARY_DIR}/protos
        PUBLIC .
)

set_target_properties(common PROPERTIES
        OUTPUT_NAME jogasaki-common-utils
        )

target_link_libraries(common
        PUBLIC jogasaki-impl
        PRIVATE Boost::boost
        PRIVATE Boost::filesystem
        PRIVATE Boost::thread
        PRIVATE Boost::container
        PRIVATE glog::glog
)

if (PERFORMANCE_TOOLS)
    target_link_libraries(common
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(common PUBLIC PERFORMANCE_TOOLS)
endif()

# Boost.Thread doesn't seem to allow multiple versions to coexist.
# This version definition should be shared with caller at least.
target_compile_definitions(common PUBLIC BOOST_THREAD_VERSION=4)

if(ENABLE_CACHE_ALIGN)
    target_compile_definitions(common INTERFACE ENABLE_CACHE_ALIGN)
endif()

set_compile_options(common)

# common library needs install only when examples are used
if(INSTALL_EXAMPLES)
    install_custom(common ${export_name})
endif()

# for tests
add_library(common-impl INTERFACE)

target_include_directories(common-impl
        INTERFACE .
        )

target_link_libraries(common-impl
        INTERFACE ${ENGINE}
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
        )

if (PERFORMANCE_TOOLS)
    target_link_libraries(common-impl
            INTERFACE performance-tools::api
            INTERFACE performance-tools::lib
            )
    target_compile_definitions(common-impl INTERFACE PERFORMANCE_TOOLS)
endif()

if(ENABLE_CACHE_ALIGN)
    target_compile_definitions(common-impl INTERFACE ENABLE_CACHE_ALIGN)
endif()

