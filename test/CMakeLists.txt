file(GLOB TEST_COMMON_SRCS
        "main.cpp"
        "jogasaki/api/api_test_base.cpp"
        "jogasaki/service_api/service_api_common.cpp"
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
        PRIVATE limestone
        )
target_include_directories(test_common
        PRIVATE .
        )

# If Arrow was vendored, the top-level CMake publishes `ARROW_INCLUDE_DIRS`
# into the cache. Tests need to see Arrow headers as well (for example
# headers such as <arrow/ipc/api.h> used by some unit tests). Add the
# include directories as SYSTEM so warnings in Arrow headers do not
# promote into -Werror failures for our test sources.
if(TARGET Arrow::arrow)
        # If Arrow exports a proper target, prefer that so include directories
        # and other usage requirements are propagated automatically.
        target_link_libraries(test_common
                        PRIVATE Arrow::arrow
        )
        if(DEFINED ARROW_INCLUDE_DIRS)
                target_include_directories(test_common
                                SYSTEM PRIVATE ${ARROW_INCLUDE_DIRS}
                )
        endif()
else()
        if(DEFINED ARROW_INCLUDE_DIRS)
                target_include_directories(test_common
                                SYSTEM PRIVATE ${ARROW_INCLUDE_DIRS}
                )
        endif()
endif()

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

        if(TARGET Arrow::arrow)
                target_link_libraries(${test_target}
                        PRIVATE Arrow::arrow
                )
                if(DEFINED ARROW_INCLUDE_DIRS)
                        # Some Arrow subproject configurations do not propagate
                        # INTERFACE_INCLUDE_DIRECTORIES through the Arrow target.
                        # As a defensive measure, add ARROW include dirs as SYSTEM
                        # includes so test sources that directly include Arrow headers
                        # compile even when linking Arrow::arrow.
                        target_include_directories(${test_target}
                                        SYSTEM PRIVATE ${ARROW_INCLUDE_DIRS}
                        )
                endif()
        else()
                if(DEFINED ARROW_INCLUDE_DIRS)
                        # Also add Arrow includes to the individual test executable so
                        # test source files that directly include Arrow headers compile.
                        target_include_directories(${test_target}
                                        SYSTEM PRIVATE ${ARROW_INCLUDE_DIRS}
                        )
                endif()
        endif()

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
        "jogasaki/service_api/*.cpp"
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

