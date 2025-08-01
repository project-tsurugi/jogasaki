file(GLOB TEST_COMMON_SRCS
        "main.cpp"
        "jogasaki/api/api_test_base.cpp"
        "jogasaki/mock/mock_task.cpp"
        "jogasaki/kvs_test_utils.cpp"
        "jogasaki/kvs/kvs_test_base.cpp"
        "jogasaki/test_utils/secondary_index.cpp"
        )

add_library(test_common OBJECT ${TEST_COMMON_SRCS})
target_link_libraries(test_common
        PUBLIC gtest
        PRIVATE jogasaki-impl
        PRIVATE common
        PRIVATE common-impl
        )
target_include_directories(test_common
        PRIVATE .
        )

function (add_test_executable source_file)
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
            )

    target_sources(${test_target}
            PRIVATE ${source_file}
            )
    add_test(
            NAME ${test_name}
            COMMAND ${test_target} --gtest_output=xml:${test_name}_gtest_result.xml
    )
endfunction (add_test_executable)

file(GLOB SRCS
        "jogasaki/*.cpp"
        "jogasaki/accessor/*.cpp"
        "jogasaki/api/*.cpp"
        "jogasaki/api/kvsservice/*.cpp"
        "jogasaki/data/*.cpp"
        "jogasaki/datastore/*.cpp"
        "jogasaki/dist/*.cpp"
        "jogasaki/error/*.cpp"
        "jogasaki/executor/*.cpp"
        "jogasaki/executor/batch/*.cpp"
        "jogasaki/executor/common/*.cpp"
        "jogasaki/executor/file/*.cpp"
        "jogasaki/executor/process/*.cpp"
        "jogasaki/executor/process/ops/*.cpp"
        "jogasaki/executor/aggregate/*.cpp"
        "jogasaki/executor/shuffle/*.cpp"
        "jogasaki/executor/sequence/*.cpp"
        "jogasaki/memory/*.cpp"
        "jogasaki/meta/*.cpp"
        "jogasaki/mock/*.cpp"
        "jogasaki/plan/*.cpp"
        "jogasaki/scheduler/*.cpp"
        "jogasaki/serializer/*.cpp"
        "jogasaki/storage/*.cpp"
        "jogasaki/kvs/*.cpp"
        "jogasaki/utils/*.cpp"
        )

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

foreach(file ${SRCS})
    if(file MATCHES "test.cpp$")
        add_test_executable(${file})
    endif()
endforeach()

