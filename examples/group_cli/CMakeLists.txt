file(GLOB SOURCES
        "*.cpp"
        "../common/*.cpp"
)

add_executable(group-cli
        ${SOURCES}
)

set_target_properties(group-cli
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "group-cli"
)

target_include_directories(group-cli
        PRIVATE .
)

target_link_libraries(group-cli
        PRIVATE jogasaki-impl
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)

if (PERFORMANCE_TOOLS)
    target_link_libraries(group-cli
            PRIVATE performance-tools::api
            PRIVATE performance-tools::lib
            )
    target_compile_definitions(group-cli PUBLIC PERFORMANCE_TOOLS)
endif()

if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(group-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(group-cli)

if(INSTALL_EXAMPLES)
    install_custom(group-cli ${export_name})
endif()

if(BUILD_TESTS)
add_test(
        NAME group-cli
        COMMAND group-cli --minimum
)
endif()
