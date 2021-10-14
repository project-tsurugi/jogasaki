/*
 * Copyright 2019-2019 tsurugi project.
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
#include <memory>
#include <string>
#include <exception>
#include <iostream>
#include <chrono>
#include <csignal>
#include <setjmp.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <tateyama/api/server/service.h>
#include <tateyama/api/endpoint/service.h>
#include <tateyama/api/endpoint/provider.h>
#include <tateyama/api/registry/registry.h>
#include <jogasaki/api.h>

#include "server.h"
#include "utils.h"

DEFINE_string(dbname, "tateyama", "database name");  // NOLINT
DEFINE_string(location, "./db", "database location on file system");  // NOLINT
DEFINE_uint32(threads, 5, "thread pool size");  //NOLINT
DEFINE_bool(remove_shm, false, "remove the shared memory prior to the execution");  // NOLINT
DEFINE_bool(load, false, "Database contents are loaded from the location just after boot");  //NOLINT
DEFINE_int32(dump_batch_size, 1024, "Batch size for dump");  //NOLINT
DEFINE_int32(load_batch_size, 1024, "Batch size for load");  //NOLINT

namespace tateyama::server {

jmp_buf buf;

void signal_handler([[maybe_unused]]int signal)
{
    VLOG(1) << sys_siglist[signal] << " signal received";
    LOG(INFO) << sys_siglist[signal] << " signal received";
    longjmp(buf, 1);
}

int backend_main(int argc, char **argv) {
    google::InitGoogleLogging("tateyama database server");

    // command arguments
    gflags::SetUsageMessage("tateyama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // database
    auto cfg = std::make_shared<jogasaki::configuration>();
    cfg->prepare_benchmark_tables(true);
    cfg->thread_pool_size(FLAGS_threads);

    auto db = jogasaki::api::create_database(cfg);
    db->start();
    DBCloser dbcloser{db};
    VLOG(1) << "database started";

    // load tpc-c tables
    if (FLAGS_load) {
        VLOG(1) << "TPC-C data load begin" << std::endl;
        std::cout << "TPC-C data load begin" << std::endl;
        try {
            tateyama::server::tpcc::load(*db, FLAGS_location);
        } catch (std::exception& e) {
            std::cerr << "[" << __FILE__ << ":" <<  __LINE__ << "] " << e.what() << std::endl;
            std::abort();
        }
        VLOG(1) << "TPC-C data load end" << std::endl;
        std::cout << "TPC-C data load end" << std::endl;
    }

    // service
    auto env = std::make_shared<tateyama::api::registry::environment>();
    auto app = tateyama::api::registry::registry<tateyama::api::server::service>::create("jogasaki");
    env->add_application(app);
    app->initialize(*env, db.get());
    auto endpoint = tateyama::api::registry::registry<tateyama::api::endpoint::provider>::create("ipc_endpoint");
    env->add_endpoint(endpoint);
    VLOG(1) << "endpoint service created" << std::endl;

    // singal handler
    std::signal(SIGINT, signal_handler);
    if (setjmp(buf) != 0) {
        endpoint->shutdown();
        app->shutdown();
        db->stop();
        return 0;
    }


    std::unordered_map<std::string, std::string> map{
        {"dbname", FLAGS_dbname},
        {"threads", std::to_string(FLAGS_threads)},

    };
    auto rc = endpoint->initialize(*env, std::addressof(map));
    return rc == status::ok ? 0 : -1;
}

}  // tateyama::server


int main(int argc, char **argv) {
    return tateyama::server::backend_main(argc, argv);
}
