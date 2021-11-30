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
#include <signal.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <tateyama/api/server/service.h>
#include <tateyama/api/endpoint/service.h>
#include <tateyama/api/endpoint/provider.h>
#include <tateyama/api/registry.h>
#include <jogasaki/api.h>

#include "server.h"
#include "../common/utils.h"

DEFINE_string(dbname, "tateyama", "database name");  // NOLINT
DEFINE_string(location, "./db", "database location on file system");  // NOLINT
DEFINE_uint32(threads, 5, "thread pool size");  //NOLINT
DEFINE_bool(remove_shm, false, "remove the shared memory prior to the execution");  // NOLINT
DEFINE_bool(load, false, "Database contents are loaded from the location just after boot");  //NOLINT
DECLARE_int32(dump_batch_size);  //NOLINT
DECLARE_int32(load_batch_size);  //NOLINT

namespace tateyama::server {

// should be in sync one in ipc_provider
struct ipc_endpoint_context {
    std::unordered_map<std::string, std::string> options_{};
    std::function<void()> database_initialize_{};
};

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

    // service
    auto env = std::make_shared<tateyama::api::environment>();
    auto app = tateyama::api::registry<tateyama::api::server::service>::create("jogasaki");
    env->add_application(app);
    app->initialize(*env, db.get());

    auto service = tateyama::api::endpoint::create_service(*env);
    env->endpoint_service(service);

    auto endpoint = tateyama::api::registry<tateyama::api::endpoint::provider>::create("ipc_endpoint");
    env->add_endpoint(endpoint);
    VLOG(1) << "endpoint service created";

    ipc_endpoint_context init_context{};
    init_context.options_ = std::unordered_map<std::string, std::string>{
        {"dbname", FLAGS_dbname},
        {"threads", std::to_string(FLAGS_threads)},
    };
    if (auto rc = endpoint->initialize(*env, std::addressof(init_context)); rc != status::ok) {
        std::abort();
    }

    if (FLAGS_load) {
        // load tpc-c tables
        VLOG(1) << "TPC-C data load begin";
        if (!VLOG_IS_ON(1)) {
            LOG(INFO) << "TPC-C data load begin";
        }
        try {
            jogasaki::common_cli::load(*db, FLAGS_location);
        } catch (std::exception& e) {
            LOG(ERROR) << " [" << __FILE__ << ":" <<  __LINE__ << "] " << e.what();
            std::abort();
        }
        VLOG(1) << "TPC-C data load end";
        if (!VLOG_IS_ON(1)) {
            LOG(INFO) << "TPC-C data load end";
        }
    }

    if (auto rc = endpoint->start(); rc != status::ok) {
        std::abort();
    }

    // wait for signal to terminate this
    int signo;
    sigset_t ss;
    sigemptyset(&ss);
    do {
        if (auto ret = sigaddset(&ss, SIGINT); ret != 0) {
            LOG(ERROR) << "fail to sigaddset";
        }
        if (auto ret = sigprocmask(SIG_BLOCK, &ss, NULL); ret != 0) {
            LOG(ERROR) << "fail to pthread_sigmask";
        }
        if (auto ret = sigwait(&ss, &signo); ret == 0) { // シグナルを待つ
            switch(signo) {
            case SIGINT:
                // termination process
                LOG(INFO) << "endpoint->shutdown()";
                endpoint->shutdown();
                LOG(INFO) << "app->shutdown()";
                app->shutdown();
                LOG(INFO) << "db->stop()";
                db->stop();
                LOG(INFO) << "exiting";
                return 0;
            }
        } else {
            LOG(ERROR) << "fail to sigwait";
            return -1;
        }
    } while(true);
}

}  // tateyama::server


int main(int argc, char **argv) {
    return tateyama::server::backend_main(argc, argv);
}
