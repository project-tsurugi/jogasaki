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

#include "../common/utils/loader.h"
#include <tateyama/api/endpoint/service.h>
#include <jogasaki/api.h>

#include "worker.h"
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

    auto db = tateyama::utils::create_database(cfg.get());
    db->start();
    DBCloser dbcloser{db};
    VLOG(1) << "database started";

    // connection channel
    auto container = std::make_unique<tateyama::common::wire::connection_container>(FLAGS_dbname);

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

    // worker objects
    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(FLAGS_threads);

    // singal handler
    std::signal(SIGINT, signal_handler);
    if (setjmp(buf) != 0) {
        for (std::size_t index = 0; index < workers.size() ; index++) {
            if (auto rv = workers.at(index)->future_.wait_for(std::chrono::seconds(0)) ; rv != std::future_status::ready) {
                VLOG(1) << "exit: remaining thread " << workers.at(index)->session_id_;
            }
        }
        workers.clear();
        db->stop();
        return 0;
    }

    // service
    auto app = tateyama::utils::create_application(db.get());
    auto service = tateyama::api::endpoint::create_service(app);
    VLOG(1) << "endpoint service created" << std::endl;

    int return_value{0};
    auto& connection_queue = container->get_connection_queue();
    while(true) {
        auto session_id = connection_queue.listen(true);
        if (connection_queue.is_terminated()) {
            VLOG(1) << "receive terminate request";
            workers.clear();
            connection_queue.confirm_terminated();
            break;
        }
        VLOG(1) << "connect request: " << session_id;
        std::string session_name = FLAGS_dbname;
        session_name += "-";
        session_name += std::to_string(session_id);
        auto wire = std::make_unique<tateyama::common::wire::server_wire_container_impl>(session_name);
        VLOG(1) << "created session wire: " << session_name;
        connection_queue.accept(session_id);
        std::size_t index;
        for (index = 0; index < workers.size() ; index++) {
            if (auto rv = workers.at(index)->future_.wait_for(std::chrono::seconds(0)) ; rv == std::future_status::ready) {
                break;
            }
        }
        if (workers.size() < (index + 1)) {
            workers.resize(index + 1);
        }
        try {
            std::unique_ptr<Worker> &worker = workers.at(index);
            worker = std::make_unique<Worker>(*service, session_id, std::move(wire));
            worker->task_ = std::packaged_task<void()>([&]{worker->run();});
            worker->future_ = worker->task_.get_future();
            worker->thread_ = std::thread(std::move(worker->task_));
        } catch (std::exception &ex) {
            LOG(ERROR) << ex.what();
            return_value = -1;
            workers.clear();
            break;
        }
    }

    db->stop();
    return return_value;
}

}  // tateyama::server


int main(int argc, char **argv) {
    return tateyama::server::backend_main(argc, argv);
}
