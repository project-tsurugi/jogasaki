file(GLOB SOURCES
        "*.cpp"
)

add_executable(service-cli
        ${SOURCES}
)

set_target_properties(service-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "service-cli"
)

target_include_directories(service-cli
        PRIVATE .
)

target_link_libraries(service-cli
        PRIVATE tateyama-impl
        PRIVATE mock
        PRIVATE takatori
        PRIVATE yugawara
        PRIVATE ${ENGINE}
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)
set_compile_options(service-cli)

target_compile_definitions(service-cli PUBLIC JOGASAKI_LIBRARY_NAME=${jogasaki_library_name})

if(INSTALL_EXAMPLES)
    install_custom(service-cli ${export_name})
endif()

# enable when pre-condition is met (jogasaki is built and installed)
#add_test(
#        NAME service-cli
#        COMMAND service-cli
#)
