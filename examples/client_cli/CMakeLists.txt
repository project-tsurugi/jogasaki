file(GLOB SOURCES
        "*.cpp"
)

add_executable(client-cli
        ${SOURCES}
)

set_target_properties(client-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "client-cli"
)

target_include_directories(client-cli
        PRIVATE .
)

target_link_libraries(client-cli
        PRIVATE jogasaki-impl #FIXME
        PRIVATE glog::glog
        PRIVATE gflags::gflags
)

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(client-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(client-cli)

if(INSTALL_EXAMPLES)
    install_custom(client-cli ${export_name})
endif()

add_test(
        NAME client-cli
        COMMAND client-cli "SELECT C0,C1 FROM T0"
)
