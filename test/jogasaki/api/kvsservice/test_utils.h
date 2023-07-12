/*
 * Copyright 2018-2023 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <tateyama/api/configuration.h>

namespace jogasaki::api::kvsservice {

constexpr std::string_view default_property {  // NOLINT
        "[sql]\n"
        "thread_pool_size=5\n"
        "lazy_worker=false\n"
        "enable_index_join=false\n"
        "stealing_enabled=true\n"
        "default_partitions=5\n"
        "stealing_wait=1\n"
        "task_polling_wait=0\n"
        "tasked_write=true\n"
        "lightweight_job_level=0\n"
        "enable_hybrid_scheduler=true\n"

        "[ipc_endpoint]\n"
        "database_name=tsurugi\n"
        "threads=104\n"
        "datachannel_buffer_size=64\n"

        "[stream_endpoint]\n"
        "port=12345\n"
        "threads=104\n"

        "[fdw]\n"
        "name=tsurugi\n"
        "threads=104\n"

        "[datastore]\n"
        "log_location=\n"
        "logging_max_parallelism=112\n"

        "[cc]\n"
        "epoch_duration=40000\n"

        "[system]\n"
        "pid_directory = /tmp\n"

};

std::shared_ptr<tateyama::api::configuration::whole> default_configuration() {
    return tateyama::api::configuration::create_configuration(
            "/work/tsurugidb/tsurugi.ini", default_property);
}

}
