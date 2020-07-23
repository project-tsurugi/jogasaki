file(GLOB SOURCES
        "jogasaki/*.cpp"
        "jogasaki/accessor/*.cpp"
        "jogasaki/api/*.cpp"
        "jogasaki/data/*.cpp"
        "jogasaki/executor/*.cpp"
        "jogasaki/executor/common/*.cpp"
        "jogasaki/executor/process/*.cpp"
        "jogasaki/executor/process/impl/*.cpp"
        "jogasaki/executor/process/impl/ops/*.cpp"
        "jogasaki/executor/process/mock/*.cpp"
        "jogasaki/executor/exchange/*.cpp"
        "jogasaki/executor/exchange/aggregate/*.cpp"
        "jogasaki/executor/exchange/broadcast/*.cpp"
        "jogasaki/executor/exchange/deliver/*.cpp"
        "jogasaki/executor/exchange/forward/*.cpp"
        "jogasaki/executor/exchange/group/*.cpp"
        "jogasaki/memory/*.cpp"
        "jogasaki/memory/details/*.cpp"
        "jogasaki/meta/*.cpp"
        "jogasaki/model/*.cpp"
        "jogasaki/plan/*.cpp"
        "jogasaki/scheduler/*.cpp"
        "jogasaki/storage/*.cpp"
        "jogasaki/utils/*.cpp"
)

add_library(jogasaki-mock
        ${SOURCES}
)

target_include_directories(jogasaki-mock
        PRIVATE .
)

target_link_libraries(jogasaki-mock
        PRIVATE jogasaki-impl
        PRIVATE Boost::boost
        PRIVATE Boost::filesystem
        PRIVATE Boost::thread
        PRIVATE Boost::container
        PRIVATE glog::glog
)

if (PERFORMANCE_TOOLS)
    target_link_libraries(jogasaki-mock
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(jogasaki-mock PUBLIC PERFORMANCE_TOOLS)
endif()

# Boost.Thread doesn't seem to allow multiple versions to coexist.
# This version definition should be shared with caller at least.
target_compile_definitions(jogasaki-mock PUBLIC BOOST_THREAD_VERSION=4)

set_compile_options(jogasaki-mock)

install_custom(jogasaki-mock ${export_name})

# for tests
add_library(jogasaki-mock-impl INTERFACE)

target_include_directories(jogasaki-mock-impl
        INTERFACE .
        )

target_link_libraries(jogasaki-mock-impl
        INTERFACE jogasaki
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
    target_link_libraries(jogasaki-mock-impl
            INTERFACE performance-tools::api
            INTERFACE performance-tools::lib
            )
    target_compile_definitions(jogasaki-impl INTERFACE PERFORMANCE_TOOLS)
endif()
