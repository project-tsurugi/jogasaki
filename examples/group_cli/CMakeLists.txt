file(GLOB SOURCES
        "*.cpp"
        "../common/*.cpp"
)

add_executable(group-cli
        ${SOURCES}
)

set_target_properties(group-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "group-cli"
)

target_include_directories(group-cli
        PRIVATE .
)

target_link_libraries(group-cli
        PRIVATE jogasaki-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
)
if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(group-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(group-cli)

if(INSTALL_EXAMPLES)
    install_custom(group-cli ${export_name})
endif()

add_test(
        NAME group-cli
        COMMAND group-cli --minimum
)
