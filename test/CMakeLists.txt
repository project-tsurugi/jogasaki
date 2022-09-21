set(test_target jogasaki-test)

set_source_files_properties(
        ${GENERATED_PROTO_SRCS}
        PROPERTIES
        GENERATED TRUE
        COMPILE_FLAGS -Wno-unused-parameter
)

add_executable(${test_target}
        main.cpp
        ${GENERATED_PROTO_SRCS}
        )

add_dependencies(${test_target}
        build_protos
        )

target_include_directories(${test_target}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_BINARY_DIR}/protos
        )

target_link_libraries(${test_target}
        PRIVATE jogasaki-impl
        PRIVATE common
        PRIVATE common-impl
        PUBLIC gtest
        PRIVATE tbb
        PRIVATE takatori
        PRIVATE atomic
        PRIVATE protobuf::libprotobuf
        PRIVATE mpdecpp
        )

function (add_test_executable source_file)
    get_filename_component(test_name "${source_file}" NAME_WE)
    target_sources(${test_target}
            PRIVATE ${source_file}
            )
    add_test(
            NAME ${test_name}
            COMMAND ${test_target} --gtest_filter=${test_name}.* --gtest_output=xml:${test_name}_gtest_result.xml
    )
endfunction (add_test_executable)

file(GLOB SRCS
        "jogasaki/*.cpp"
        "jogasaki/accessor/*.cpp"
        "jogasaki/api/*.cpp"
        "jogasaki/data/*.cpp"
        "jogasaki/executor/*.cpp"
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
        "jogasaki/scheduler/*.cpp"
        "jogasaki/serializer/*.cpp"
        "jogasaki/kvs/*.cpp"
        "jogasaki/third_party/*.cpp"
        "jogasaki/utils/*.cpp"
        )

# long tx specific testcases
if(BUILD_SHIRAKAMI_WP AND ${SHARKSFIN_IMPLEMENTATION} STREQUAL "shirakami")
    # temporarily disable cc related test due to unstability
#    file(GLOB CC_TESTS
#            "jogasaki/cc/*.cpp"
#            )
#    foreach(file ${CC_TESTS})
#        list(APPEND SRCS ${file})
#    endforeach()
    file(GLOB LONG_TX_TESTS
            "jogasaki/long_tx/*.cpp"
            )
    foreach(file ${LONG_TX_TESTS})
        list(APPEND SRCS ${file})
    endforeach()
endif()

if(LOGSHIP)
    file(GLOB LOGSHIP_TESTS
            "jogasaki/logship/*.cpp"
            )
    foreach(file ${LOGSHIP_TESTS})
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
    add_test_executable(${file})
endforeach()

