set(test_target jogasaki-test)

add_executable(${test_target}
        main.cpp
        )

target_include_directories(${test_target}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        )

target_link_libraries(${test_target}
        PRIVATE jogasaki-impl
        PRIVATE jogasaki-mock
        PRIVATE jogasaki-mock-impl
        PUBLIC gtest
        PRIVATE tbb
        PRIVATE takatori
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
        "jogasaki/executor/process/*.cpp"
        "jogasaki/executor/process/ops/*.cpp"
        "jogasaki/executor/shuffle/*.cpp"
        "jogasaki/memory/*.cpp"
        "jogasaki/meta/*.cpp"
        "jogasaki/mock/*.cpp"
        "jogasaki/scheduler/*.cpp"
        "jogasaki/storage/*.cpp"
        "jogasaki/utils/*.cpp"
        )

foreach(file ${SRCS})
    add_test_executable(${file})
endforeach()

