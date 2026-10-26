# static library shared by the tools calling sql service in-process (service_cli, service_benchmark)
add_library(service-common STATIC
        service_client.cpp
        service_setup.cpp
        utils.cpp
)

target_include_directories(service-common
        PUBLIC .
)

target_link_libraries(service-common
        PUBLIC api
        PUBLIC ${tateyama_engine}
        PUBLIC common
        PUBLIC takatori
        PUBLIC yugawara
        PUBLIC ${ENGINE}
        PUBLIC glog::glog
        PUBLIC gflags::gflags
        PUBLIC Boost::filesystem
)

set_compile_options(service-common)
