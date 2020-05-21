file(GLOB SOURCES
        "*.cpp"
        "executor/*.cpp"
        "executor/common/*.cpp"
        "executor/process/*.cpp"
        "executor/exchange/*.cpp"
        "executor/exchange/broadcast/*.cpp"
        "executor/exchange/deliver/*.cpp"
        "executor/exchange/forward/*.cpp"
        "executor/exchange/group/*.cpp"
        "model/*.cpp"
        "scheduler/*.cpp"
        "data/*.cpp"
)

add_library(jogasaki
    ${SOURCES}
)

target_include_directories(jogasaki
        PUBLIC .
)

target_link_libraries(jogasaki
        PRIVATE takatori
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
