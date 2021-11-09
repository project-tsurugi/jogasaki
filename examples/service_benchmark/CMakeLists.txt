file(GLOB SOURCES
        "*.cpp"
        "../common/utils.cpp"
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
        RUNTIME_OUTPUT_NAME "cli"
)

target_include_directories(service-benchmark
        PRIVATE .
)

target_link_libraries(service-benchmark
        PRIVATE api
        PRIVATE ${tateyama_engine}
        PRIVATE common
        PRIVATE takatori
        PRIVATE yugawara
        PRIVATE ${ENGINE}
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        PRIVATE Threads::Threads
)

set_compile_options(service-benchmark)

if(INSTALL_EXAMPLES)
    install_custom(service-benchmark ${export_name})
endif()

add_test(
        NAME service-benchmark
        COMMAND cli --minimum
)
