file(GLOB SOURCES
        "jogasaki/executor/exchange/mock/aggregate/*.cpp"
        "jogasaki/executor/process/mock/*.cpp"
        "jogasaki/mock/*.cpp"
)

add_library(jogasaki-mock
        ${SOURCES}
)

target_include_directories(jogasaki-mock
        PRIVATE .
)

# avoid name conflict with mock sharksfin implementation
set_target_properties(jogasaki-mock PROPERTIES
        OUTPUT_NAME jogasaki-mock-utils
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

if(ENABLE_CACHE_ALIGN)
    target_compile_definitions(jogasaki-mock INTERFACE ENABLE_CACHE_ALIGN)
endif()

set_compile_options(jogasaki-mock)

install_custom(jogasaki-mock ${export_name})

# for tests
add_library(jogasaki-mock-impl INTERFACE)

target_include_directories(jogasaki-mock-impl
        INTERFACE .
        )

target_link_libraries(jogasaki-mock-impl
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
    target_link_libraries(jogasaki-mock-impl
            INTERFACE performance-tools::api
            INTERFACE performance-tools::lib
            )
    target_compile_definitions(jogasaki-impl INTERFACE PERFORMANCE_TOOLS)
endif()

if(ENABLE_CACHE_ALIGN)
    target_compile_definitions(jogasaki-mock-impl INTERFACE ENABLE_CACHE_ALIGN)
endif()

