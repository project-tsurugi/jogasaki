file(GLOB SOURCES
        "*.cpp"
        "../common/*.cpp"
)

add_executable(mock-aggregate-cli
        ${SOURCES}
)

set_target_properties(mock-aggregate-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "mock-aggregate-cli"
)

target_include_directories(mock-aggregate-cli
        PRIVATE .
)

target_link_libraries(mock-aggregate-cli
        PRIVATE jogasaki-impl
        PRIVATE common
        PRIVATE common-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)

if (PERFORMANCE_TOOLS)
    target_link_libraries(mock-aggregate-cli
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(mock-aggregate-cli PUBLIC PERFORMANCE_TOOLS)
endif()

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(mock-aggregate-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(mock-aggregate-cli)

if(INSTALL_EXAMPLES)
    install_custom(mock-aggregate-cli ${export_name})
endif()

add_test(
        NAME mock-aggregate-cli
        COMMAND mock-aggregate-cli --minimum
)
