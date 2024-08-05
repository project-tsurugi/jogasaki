add_subdirectory(group_cli)
add_subdirectory(cogroup_cli)
add_subdirectory(mock_aggregate_cli)
add_subdirectory(sql_cli)
add_subdirectory(process_cli)
add_subdirectory(join_cli)
add_subdirectory(aggregate_cli)
add_subdirectory(client_cli)
add_subdirectory(query_bench_cli)
add_subdirectory(service_benchmark)

# examples requiring perf-tools
if(PERFORMANCE_TOOLS)
add_subdirectory(scan_cli)
endif()

if(EXISTS "../third_party/linenoise-ng/src")
add_subdirectory(service_cli)
endif()
