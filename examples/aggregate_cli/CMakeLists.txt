file(GLOB SOURCES
        "*.cpp"
        "../common/*.cpp"
)

add_executable(aggregate-cli
        ${SOURCES}
)

set_target_properties(aggregate-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "aggregate-cli"
)

target_include_directories(aggregate-cli
        PRIVATE .
)

target_link_libraries(aggregate-cli
        PRIVATE jogasaki-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
)

if (PERFORMANCE_TOOLS)
    target_link_libraries(aggregate-cli
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(aggregate-cli PUBLIC PERFORMANCE_TOOLS)
endif()

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(aggregate-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(aggregate-cli)

if(INSTALL_EXAMPLES)
    install_custom(aggregate-cli ${export_name})
endif()

add_test(
        NAME aggregate-cli
        COMMAND aggregate-cli --minimum
)
