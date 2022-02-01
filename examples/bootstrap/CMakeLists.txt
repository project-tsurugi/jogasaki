# Copyright 2019-2021 tsurugi project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

file(GLOB SOURCES
    "*.cpp"
    "../common/utils.cpp"
    "../common/tpch_utils.cpp"
)

add_executable(server
    ${SOURCES}
)

set_target_properties(server
    PROPERTIES
        RUNTIME_OUTPUT_NAME "tateyama-server"
)

target_include_directories(server
    PRIVATE ${CMAKE_BINARY_DIR}/protos
    PRIVATE .
)

target_link_libraries(server
    PRIVATE api
    PRIVATE ${tateyama_engine}
    PRIVATE common
    PRIVATE takatori
    PRIVATE yugawara
    PRIVATE ${ENGINE}
    PRIVATE glog::glog
    PRIVATE gflags::gflags
    PRIVATE pthread
    PRIVATE Boost::filesystem
    PRIVATE rt
)
set_compile_options(server)

if(INSTALL_EXAMPLES)
    install_custom(server ${export_name})
endif()
