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
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE performance-tools::api
        PRIVATE performance-tools::lib
)
if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(cogroup-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(cogroup-cli)

if(INSTALL_EXAMPLES)
    install_custom(cogroup-cli ${export_name})
endif()

add_test(
        NAME cogroup-cli
        COMMAND cogroup-cli
)
