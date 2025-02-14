file(GLOB SOURCES
        "*.cpp"
        "../common/*.cpp"
)

add_executable(aggregate-cli
        ${SOURCES}
)

set_target_properties(aggregate-cli
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "aggregate-cli"
)

target_include_directories(aggregate-cli
        PRIVATE .
)

target_link_libraries(aggregate-cli
        PRIVATE jogasaki-impl
        PRIVATE common
        PRIVATE common-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(aggregate-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(aggregate-cli)

if(INSTALL_EXAMPLES)
    install_custom(aggregate-cli ${export_name})
endif()

if(BUILD_TESTS)
add_test(
        NAME aggregate-cli
        COMMAND aggregate-cli --minimum
)
endif()
