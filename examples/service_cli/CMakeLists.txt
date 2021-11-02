file(GLOB SOURCES
        "*.cpp"
        "../common/utils.cpp"
        "../../third_party/linenoise-ng/src/*.cpp"
)

set_source_files_properties(
        ${SOURCES}
        PROPERTIES
        COMPILE_FLAGS -Wno-implicit-fallthrough
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
        PRIVATE ../../third_party/linenoise-ng/include
)

target_link_libraries(service-cli
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
set_compile_options(service-cli)

if(INSTALL_EXAMPLES)
    install_custom(service-cli ${export_name})
endif()

add_test(
        NAME service-cli
        COMMAND service-cli --test_build
)
