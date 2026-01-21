file(GLOB TEST_COMMON_SRCS
        "main.cpp"
        "jogasaki/api/api_test_base.cpp"
        "jogasaki/service_api/service_api_common.cpp"
        "jogasaki/mock/mock_task.cpp"
        "jogasaki/kvs_test_utils.cpp"
        "jogasaki/kvs/kvs_test_base.cpp"
        "jogasaki/test_utils/create_configuration.cpp"
        "jogasaki/test_utils/secondary_index.cpp"
        )

add_library(test_common OBJECT ${TEST_COMMON_SRCS})
target_link_libraries(test_common
        PUBLIC gtest
        PRIVATE jogasaki-impl
        PRIVATE common
        PRIVATE common-impl
        PRIVATE limestone
        )
target_include_directories(test_common
        PRIVATE .
        )

function (add_test_executable source_file serial_execution)
    get_filename_component(test_name "${source_file}" NAME_WE)
    set(test_target ${test_name})
    add_executable(${test_target})
    add_dependencies(${test_target}
            build_protos
            )

    target_include_directories(${test_target}
            PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
            )

    target_link_libraries(${test_target}
            PRIVATE test_common
            PRIVATE jogasaki-impl
            PRIVATE common
            PRIVATE common-impl
            PUBLIC gtest
            PRIVATE tbb
            PRIVATE takatori
            PRIVATE mizugaki
            PRIVATE limestone
            PRIVATE atomic
            PRIVATE protobuf::libprotobuf
            PRIVATE mpdecpp
            PRIVATE data-relay-grpc
            PRIVATE gRPC::grpc++
            PRIVATE absl_synchronization
            )

    target_sources(${test_target}
            PRIVATE ${source_file}
            )
    add_test(
            NAME ${test_name}
            COMMAND ${test_target} --gtest_output=xml:${test_name}_gtest_result.xml
    )
    if(serial_execution)
        set_tests_properties(${test_name} PROPERTIES RESOURCE_LOCK framework_server)
    endif()
endfunction (add_test_executable)

file(GLOB SRCS
        "jogasaki/*.cpp"
        "jogasaki/accessor/*.cpp"
        "jogasaki/auth/*.cpp"
        "jogasaki/api/*.cpp"
        "jogasaki/api/kvsservice/*.cpp"
        "jogasaki/data/*.cpp"
        "jogasaki/datastore/*.cpp"
        "jogasaki/dist/*.cpp"
        "jogasaki/error/*.cpp"
        "jogasaki/executor/*.cpp"
        "jogasaki/executor/batch/*.cpp"
        "jogasaki/executor/common/*.cpp"
        "jogasaki/executor/dto/*.cpp"
        "jogasaki/executor/file/*.cpp"
        "jogasaki/executor/io/*.cpp"
        "jogasaki/executor/process/*.cpp"
        "jogasaki/executor/process/ops/*.cpp"
        "jogasaki/executor/aggregate/*.cpp"
        "jogasaki/executor/shuffle/*.cpp"
        "jogasaki/executor/sequence/*.cpp"
        "jogasaki/memory/*.cpp"
        "jogasaki/meta/*.cpp"
        "jogasaki/mock/*.cpp"
        "jogasaki/plan/*.cpp"
        "jogasaki/relay/*.cpp"
        "jogasaki/scheduler/*.cpp"
        "jogasaki/serializer/*.cpp"
        "jogasaki/service_api/*.cpp"
        "jogasaki/storage/*.cpp"
        "jogasaki/kvs/*.cpp"
        "jogasaki/utils/*.cpp"
        )

if(${SHARKSFIN_IMPLEMENTATION} STREQUAL "memory")
    # exclusive_ddl_dml test uses many txns and goes into infinite loop with jogasaki-memory
    list(FILTER SRCS EXCLUDE REGEX ".*/exclusive_ddl_dml[^/]*$")
    # sql_lob_function_invocation depends on limestone and data-relay-grpc service and somehow fails with jogasaki-memory
    list(FILTER SRCS EXCLUDE REGEX ".*/sql_lob_function_invocation[^/]*$")
    # sql_apply_blob_test depends on limestone and data-relay-grpc service and somehow fails with jogasaki-memory
    list(FILTER SRCS EXCLUDE REGEX ".*/sql_apply_blob[^/]*$")
endif()

# long tx specific testcases
if(${SHARKSFIN_IMPLEMENTATION} STREQUAL "shirakami")
    # temporarily disable cc related test due to unstability
    file(GLOB CC_TESTS
            "jogasaki/cc/*.cpp"
            )
    foreach(file ${CC_TESTS})
        list(APPEND SRCS ${file})
    endforeach()
    file(GLOB LONG_TX_TESTS
            "jogasaki/long_tx/*.cpp"
            )
    foreach(file ${LONG_TX_TESTS})
        list(APPEND SRCS ${file})
    endforeach()
endif()

# add implementation specific testcases
file(GLOB ADDED
        "jogasaki/targets/${SHARKSFIN_IMPLEMENTATION}/*.cpp"
        )
foreach(file ${ADDED})
    list(APPEND SRCS ${file})
endforeach()

# List of test files that use framework::server and require serial execution
set(SERIAL_EXECUTION_TESTS
        "jogasaki/api/framework_test.cpp"
        "jogasaki/api/kvsservice/server_test.cpp"
        "jogasaki/api/kvsservice/mock_server_test.cpp"
        "jogasaki/api/kvsservice/kvsservice_bench_test.cpp"
        "jogasaki/api/kvsservice/kvs_tx_test.cpp"
        "jogasaki/api/sql_apply_blob_test.cpp"
        "jogasaki/api/sql_lob_function_invocation_test.cpp"
        "jogasaki/relay/blob_session_container_test.cpp"
        "jogasaki/relay/blob_session_test.cpp"
        "jogasaki/service_api/service_api_apply_lob_test.cpp"
        )

foreach(file ${SRCS})
    if(file MATCHES "test.cpp$")
        # Check if this test requires serial execution
        set(requires_serial_execution FALSE)
        foreach(serial_test ${SERIAL_EXECUTION_TESTS})
            if(file MATCHES "${serial_test}$")
                set(requires_serial_execution TRUE)
                break()
            endif()
        endforeach()

        add_test_executable(${file} ${requires_serial_execution})
    endif()
endforeach()

