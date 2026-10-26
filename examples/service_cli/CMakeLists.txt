file(GLOB SOURCES
        "*.cpp"
        "../../third_party/linenoise-ng/src/*.cpp"
)

set_source_files_properties(
        ${SOURCES}
        PROPERTIES
        COMPILE_FLAGS "-Wno-implicit-fallthrough -Wno-c++20-compat -Wno-stringop-overflow"
)

add_executable(service-cli
        ${SOURCES}
)

set_target_properties(service-cli
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "cli"
)

target_include_directories(service-cli
        PRIVATE .
        PRIVATE ../../third_party/linenoise-ng/include
)

target_link_libraries(service-cli
        PRIVATE service-common
        PRIVATE Threads::Threads
)
set_compile_options(service-cli)

if(INSTALL_EXAMPLES)
    install_custom(service-cli ${export_name})
endif()

if(BUILD_TESTS)
add_test(
        NAME service-cli
        COMMAND cli --test_build
)
# run bundled scripts - they are expected to complete without unexpected errors
file(GLOB SERVICE_CLI_SCRIPTS "${CMAKE_CURRENT_SOURCE_DIR}/scripts/*.sql")
foreach(script ${SERVICE_CLI_SCRIPTS})
    get_filename_component(script_name ${script} NAME_WE)
    add_test(
            NAME service-cli-script-${script_name}
            COMMAND cli --input_file ${script} --timestamp=false --thread_count=1 --partitions=1
    )
endforeach()
endif()
