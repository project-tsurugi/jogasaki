/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "create_configuration.h"

#include <memory>
#include <sstream>
#include <string>
#include <string_view>

#include <boost/filesystem.hpp>

#include <tateyama/api/configuration.h>

namespace jogasaki::test_utils {

static constexpr std::string_view default_configuration {  // NOLINT
    "[sql]\n"
        "thread_pool_size=\n"
        "enable_index_join=false\n"
        "stealing_enabled=true\n"
        "default_partitions=5\n"
        "stealing_wait=1\n"
        "task_polling_wait=0\n"
        "lightweight_job_level=0\n"
        "enable_hybrid_scheduler=true\n"

    "[ipc_endpoint]\n"
        "database_name=tsurugi\n"
        "threads=104\n"
        "datachannel_buffer_size=64\n"
        "max_datachannel_buffers=256\n"
        "admin_sessions=1\n"
        "allow_blob_privileged=true\n"

    "[stream_endpoint]\n"
        "enabled=false\n"
        "port=12345\n"
        "threads=104\n"
        "allow_blob_privileged=false\n"
        "dev_idle_work_interval=1000\n"

    "[cc]\n"
        "epoch_duration=40000\n"
        "waiting_resolver_threads=2\n"

    "[authentication]\n"
        "enabled=false\n"
        "url=http://localhost:8080/harinoki\n"
        "request_timeout=0\n"

    "[grpc_server]\n"
        "enabled=true\n"
        "listen_address=0.0.0.0:52345\n"
        "endpoint=dns:///localhost:52345\n"
        "secure=false\n"

    "[blob_relay]\n"
        "enabled=true\n"
        "session_store=unset\n"
        "session_quota_size=\n"
        "local_enabled=true\n"
        "local_upload_copy_file=false\n"
        "stream_chunk_size=1048576\n"
        "dev_accept_mock_tag=true\n"

    "[datastore]\n"
        "logging_max_parallelism=112\n"
        "log_location=unset"
};

std::shared_ptr<tateyama::api::configuration::whole> create_configuration(
    std::string const& log_location,
    std::string const& session_store
) {
    std::stringstream ss{};
    ss << default_configuration;
    ss << "\n";
    auto cfg = tateyama::api::configuration::create_configuration("", ss.str());

    {
        boost::filesystem::create_directory(log_location);
        cfg->get_section("datastore")->set("log_location", log_location);
    }
    {
        boost::filesystem::create_directory(session_store);
        cfg->get_section("blob_relay")->set("session_store", session_store);
    }
    return cfg;
}

}  // namespace jogasaki::test_utils
