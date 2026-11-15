file(GLOB SOURCES
        "*.cpp"
)

set_source_files_properties(
        ${SOURCES}
        PROPERTIES
        COMPILE_FLAGS -Wno-implicit-fallthrough
)

add_executable(service-benchmark
        ${SOURCES}
)

set_target_properties(service-benchmark
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "cli"
)

target_include_directories(service-benchmark
        PRIVATE .
)

target_link_libraries(service-benchmark
        PRIVATE service-common
        PRIVATE Threads::Threads
)

set_compile_options(service-benchmark)

if(INSTALL_EXAMPLES)
    install_custom(service-benchmark ${export_name})
endif()

if(BUILD_TESTS)
add_test(
        NAME service-benchmark
        COMMAND cli --minimum
)
# run each mode shortly to verify it works
set(SERVICE_BENCHMARK_TEST_ARGS --transactions=1 --statements=10 --thread_count=1 --partitions=1 --prepare_data=10)
foreach(mode insert upsert update query query2)
    add_test(
            NAME service-benchmark-${mode}
            COMMAND cli --${mode} ${SERVICE_BENCHMARK_TEST_ARGS}
    )
endforeach()
add_test(
        NAME service-benchmark-update-ltx
        COMMAND cli --update --ltx ${SERVICE_BENCHMARK_TEST_ARGS}
)
add_test(
        NAME service-benchmark-query-rtx
        COMMAND cli --query --rtx ${SERVICE_BENCHMARK_TEST_ARGS}
)
add_test(
        NAME service-benchmark-insert-ddl
        COMMAND cli --insert --ddl --secondary --transactions=1 --statements=10 --thread_count=1 --partitions=1
)
add_test(
        NAME service-benchmark-query-clients
        COMMAND cli --query --clients=2 --thread_count=2 --transactions=1 --statements=10 --partitions=1 --prepare_data=10
)
endif()
