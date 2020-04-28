file(GLOB SOURCES
    "*.cpp"
)

add_executable(cli
        ${SOURCES}
)

set_target_properties(cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "dc-cli"
)

target_include_directories(cli
        PRIVATE .
)

target_link_libraries(cli
        PRIVATE takatori
        PRIVATE dag_controller
        PRIVATE glog::glog
        PRIVATE gflags::gflags
)
if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(cli)

if(INSTALL_EXAMPLES)
    install_custom(cli ${export_name})
endif()

add_test(
        NAME cli
        COMMAND cli
)
