file(GLOB SOURCES
        "*.cpp"
        "../common/*.cpp"
)

add_executable(cogroup-cli
        ${SOURCES}
)

set_target_properties(cogroup-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "cogroup-cli"
)

target_include_directories(cogroup-cli
        PRIVATE .
)

target_link_libraries(cogroup-cli
        PRIVATE jogasaki-impl
        PRIVATE common
        PRIVATE common-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)

if (PERFORMANCE_TOOLS)
    target_link_libraries(cogroup-cli
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(cogroup-cli PUBLIC PERFORMANCE_TOOLS)
endif()

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(cogroup-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(cogroup-cli)

if(INSTALL_EXAMPLES)
    install_custom(cogroup-cli ${export_name})
endif()

if(BUILD_TESTS)
add_test(
        NAME cogroup-cli
        COMMAND cogroup-cli --minimum
)
endif()
