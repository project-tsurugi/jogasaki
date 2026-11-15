file(GLOB SOURCES
        "*.cpp"
)

add_executable(nsbench
        ${SOURCES}
)

set_target_properties(nsbench
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "nsbench"
)

target_include_directories(nsbench
        PRIVATE .
)

target_link_libraries(nsbench
        PRIVATE service-common
        PRIVATE Threads::Threads
)

set_compile_options(nsbench)

if(INSTALL_EXAMPLES)
    install_custom(nsbench ${export_name})
endif()

if(BUILD_TESTS)
# run each command shortly to verify it works
set(NSBENCH_TEST_ARGS --thread_count=1 --partitions=1 --operations=10)
add_test(
        NAME nsbench-lkv
        COMMAND nsbench prepare insert update select delete noptx ${NSBENCH_TEST_ARGS}
)
add_test(
        NAME nsbench-sysbench
        COMMAND nsbench prepare insert update select delete -e sysbench ${NSBENCH_TEST_ARGS}
)
add_test(
        NAME nsbench-st
        COMMAND nsbench prepare insert-st update-st select-st noptx-st --threads=2 ${NSBENCH_TEST_ARGS}
)
add_test(
        NAME nsbench-mt
        COMMAND nsbench prepare insert-mt update-mt select-mt delete-mt --tables=2 ${NSBENCH_TEST_ARGS}
)
add_test(
        NAME nsbench-ltx
        COMMAND nsbench prepare insert update select -t LTX --read_area inclusive ${NSBENCH_TEST_ARGS}
)
endif()
