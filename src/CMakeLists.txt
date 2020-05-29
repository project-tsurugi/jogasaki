file(GLOB SOURCES
        "jogasaki/*.cpp"
        "jogasaki/executor/*.cpp"
        "jogasaki/executor/common/*.cpp"
        "jogasaki/executor/process/*.cpp"
        "jogasaki/executor/exchange/*.cpp"
        "jogasaki/executor/exchange/broadcast/*.cpp"
        "jogasaki/executor/exchange/deliver/*.cpp"
        "jogasaki/executor/exchange/forward/*.cpp"
        "jogasaki/executor/exchange/group/*.cpp"
        "jogasaki/model/*.cpp"
        "jogasaki/scheduler/*.cpp"
        "jogasaki/data/*.cpp"
)

add_library(jogasaki
        ${SOURCES}
)

target_include_directories(jogasaki
        PRIVATE .
)

target_link_libraries(jogasaki
        PUBLIC api
        PRIVATE takatori
        PRIVATE yugawara
        PRIVATE mizugaki
        PRIVATE shakujo-model
        PRIVATE shakujo-parser
        PRIVATE tbb
        PRIVATE numa
        PRIVATE Boost::boost
        PRIVATE Boost::filesystem
        PRIVATE Boost::thread
        PRIVATE Boost::container
        PRIVATE glog::glog
)

# Boost.Thread doesn't seem to allow multiple versions to coexist.
# This version definition should be shared with caller at least.
target_compile_definitions(jogasaki PUBLIC BOOST_THREAD_VERSION=4)

set_compile_options(jogasaki)

install_custom(jogasaki ${export_name})

# for tests
add_library(jogasaki-impl INTERFACE)

target_include_directories(jogasaki-impl
        INTERFACE .
        )

target_link_libraries(jogasaki-impl
        INTERFACE jogasaki
        INTERFACE takatori
        INTERFACE yugawara
        INTERFACE mizugaki
        INTERFACE shakujo-model
        INTERFACE shakujo-parser
        INTERFACE tbb
        INTERFACE numa
        INTERFACE Boost::boost
        INTERFACE Boost::filesystem
        INTERFACE Boost::thread
        INTERFACE Boost::container
        INTERFACE glog::glog
        )
