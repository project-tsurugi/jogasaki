file(GLOB SOURCES
        "*.cpp"
        "../common/*.cpp"
)

add_executable(join-cli
        ${SOURCES}
)

set_target_properties(join-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "join-cli"
)

target_include_directories(join-cli
        PRIVATE .
)

target_link_libraries(join-cli
        PRIVATE jogasaki-impl
        PRIVATE jogasaki-mock
        PRIVATE jogasaki-mock-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
)

target_link_libraries(join-cli
        PRIVATE performance-tools::api
        PRIVATE performance-tools::lib
        )

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(join-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(join-cli)

if(INSTALL_EXAMPLES)
    install_custom(join-cli ${export_name})
endif()

add_test(
        NAME join-cli
        COMMAND join-cli --minimum
)
