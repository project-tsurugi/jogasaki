file(GLOB SOURCES
        "*.cpp"
)

add_executable(scan-cli
        ${SOURCES}
)

set_target_properties(scan-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "scan-cli"
)

target_include_directories(scan-cli
        PRIVATE .
)

target_link_libraries(scan-cli
        PRIVATE jogasaki-impl #FIXME
        PRIVATE jogasaki-mock-impl #FIXME
        PRIVATE glog::glog
        PRIVATE gflags::gflags
)

if (PERFORMANCE_TOOLS)
    target_link_libraries(scan-cli
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(scan-cli PUBLIC PERFORMANCE_TOOLS)
endif()

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(scan-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(scan-cli)

if(INSTALL_EXAMPLES)
    install_custom(scan-cli ${export_name})
endif()

add_test(
        NAME scan-cli
        COMMAND scan-cli --minimum
)
