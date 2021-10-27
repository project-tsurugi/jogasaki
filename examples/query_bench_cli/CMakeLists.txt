file(GLOB SOURCES
        "*.cpp"
)

add_executable(query-bench-cli
        ${SOURCES}
)

set_target_properties(query-bench-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "query-bench-cli"
)

target_include_directories(query-bench-cli
        PRIVATE .
)

target_link_libraries(query-bench-cli
        PRIVATE jogasaki-impl #FIXME
        PRIVATE common
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
        PRIVATE Boost::filesystem
)
if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(query-bench-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(query-bench-cli)

if(INSTALL_EXAMPLES)
    install_custom(query-bench-cli ${export_name})
endif()

add_test(
        NAME query-bench-cli
        COMMAND query-bench-cli --minimum --location TMP
)
