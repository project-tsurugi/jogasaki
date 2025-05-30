file(GLOB SOURCES
        "*.cpp"
)

add_executable(process-cli
        ${SOURCES}
)

set_target_properties(process-cli
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "process-cli"
)

target_include_directories(process-cli
        PRIVATE .
)

target_link_libraries(process-cli
        PRIVATE jogasaki-impl #FIXME
        PRIVATE common
        PRIVATE common-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)

if (PERFORMANCE_TOOLS)
    target_link_libraries(process-cli
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(process-cli PUBLIC PERFORMANCE_TOOLS)
endif()

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(process-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(process-cli)

if(INSTALL_EXAMPLES)
    install_custom(process-cli ${export_name})
endif()

if(BUILD_TESTS)
add_test(
        NAME process-cli
        COMMAND process-cli --minimum
)
endif()
