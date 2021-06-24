file(GLOB SOURCES
        "*.cpp"
)

add_executable(task-scheduler-cli
        ${SOURCES}
)

set_target_properties(task-scheduler-cli
        PROPERTIES
        RUNTIME_OUTPUT_NAME "task-scheduler-cli"
)

target_include_directories(task-scheduler-cli
        PRIVATE .
)

target_link_libraries(task-scheduler-cli
        PRIVATE jogasaki-impl #FIXME
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)
if (ENABLE_GOOGLE_PERFTOOLS)
    target_link_libraries(task-scheduler-cli
            PRIVATE gperftools::gperftools
            )
endif()

set_compile_options(task-scheduler-cli)

if(INSTALL_EXAMPLES)
    install_custom(task-scheduler-cli ${export_name})
endif()

add_test(
        NAME task-scheduler-cli
        COMMAND task-scheduler-cli --minimum
)
