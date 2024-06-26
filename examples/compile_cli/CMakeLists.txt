file(GLOB SOURCES
        "*.cpp"
)

add_executable(compile-cli
        ${SOURCES}
)

set_target_properties(compile-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "compile-cli"
)

target_include_directories(compile-cli
        PRIVATE .
)

target_link_libraries(compile-cli
        PRIVATE jogasaki-impl
        PRIVATE mizugaki
        PRIVATE shakujo-model
        PRIVATE shakujo-parser
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)
if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(compile-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(compile-cli)

if(INSTALL_EXAMPLES)
    install_custom(compile-cli ${export_name})
endif()

if(BUILD_TESTS)
add_test(
        NAME compile-cli
        COMMAND compile-cli "SELECT C1 FROM T0"
)
endif()
