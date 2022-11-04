file(GLOB SOURCES
        "*.cpp"
)

add_executable(sql-cli
        ${SOURCES}
)

set_target_properties(sql-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "sql-cli"
)

target_include_directories(sql-cli
        PRIVATE .
)

target_link_libraries(sql-cli
        PRIVATE jogasaki-impl #FIXME
        PRIVATE common
        PRIVATE common-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
        PRIVATE Boost::filesystem
)
if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(sql-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(sql-cli)

if(INSTALL_EXAMPLES)
    install_custom(sql-cli ${export_name})
endif()

add_test(
        NAME sql-cli
        COMMAND sql-cli --v=50 --location TMP "SELECT C0,C1 FROM T0"
)
