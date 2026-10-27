/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "service_setup.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <sharksfin/DatabaseOptions.h>
#include <sharksfin/api.h>
#include <takatori/util/downcast.h>
#include <tateyama/api/configuration.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/add_test_tables.h>
#include <jogasaki/utils/storage_data.h>

#include "utils.h"

// Flags common to the tools running sql service in-process.
// Defaults are same as jogasaki::configuration. Each tool can override them by set_flag_defaults().
// These definitions must stay in the same translation unit as fill_configuration() so that they are not dropped
// when linking this static library.
DEFINE_bool(single_thread, false, "Whether to run on serial scheduler");  //NOLINT
DEFINE_int32(thread_count, 5, "Number of threads used in server thread pool");  //NOLINT
DEFINE_bool(core_affinity, false, "Whether worker threads are assigned to cores sequentially from --initial_core. Exclusive with --assign_numa_nodes_uniformly and --force_numa_node");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, false, "Whether worker threads are assigned to numa nodes uniformly. Exclusive with --core_affinity and --force_numa_node");  //NOLINT
DEFINE_int32(force_numa_node, -1, "assign all worker threads to the specified numa node. Specify -1 not to use. Exclusive with --core_affinity and --assign_numa_nodes_uniformly");  //NOLINT
DEFINE_int32(partitions, 5, "Number of partitions per process");  //NOLINT
DEFINE_bool(steal, true, "Enable stealing for task scheduling");  //NOLINT
DEFINE_int32(stealing_wait, 1, "Coefficient for the number of times checking local queue before stealing");  //NOLINT
DEFINE_int32(task_polling_wait, 0, "wait method/duration parameter in the worker's busy loop");  //NOLINT
DEFINE_bool(use_preferred_worker_for_current_thread, true, "whether worker is selected depending on the current thread requesting schedule");  //NOLINT
DEFINE_bool(enable_hybrid_scheduler, true, "enable serial-stealing hybrid scheduler");  //NOLINT
DEFINE_int32(lightweight_job_level, 0, "Specify job level regarded as lightweight");  //NOLINT
DEFINE_bool(busy_worker, false, "whether task scheduler workers suspend when they have no task. Specify true to stop suspend.");  //NOLINT
DEFINE_int64(watcher_interval, 1000, "duration in us before watcher thread wakes up in order to try next check");  //NOLINT
DEFINE_int64(worker_try_count, 1000, "how many times worker checks the task queues before suspend");  //NOLINT
DEFINE_int64(worker_suspend_timeout, 1000000, "duration in us before worker wakes up from suspend");  //NOLINT
DEFINE_int64(scan_block_size, 100, "max records processed by scan operator before yielding to other tasks");  //NOLINT
DEFINE_int64(scan_yield_interval, 1, "max time (ms) processed by scan operator before yielding to other tasks");  //NOLINT
DEFINE_int64(scan_default_parallel, 4, "max parallel execution count of scan tasks");  //NOLINT
DEFINE_int64(max_result_set_writers, 64, "max number of result set writers");  //NOLINT
DEFINE_string(commit_response, "", "default commit response: accepted, available, stored or propagated. Empty to use the default (stored)");  //NOLINT
DEFINE_int64(epoch_duration, 0, "epoch duration (us) of the kvs. Specify 0 to use the kvs default (40000us for shirakami)");  //NOLINT
DEFINE_string(location, "TMP", "specify the database directory. Pass TMP to use temporary directory.");  //NOLINT
DEFINE_bool(builtin_tables, false, "register built-in test tables (T0, T1, T2, ...) and benchmark tables (TPC-C like)");  //NOLINT
DEFINE_string(load_from, "", "specify the generated db file directory to load initial data. Requires --builtin_tables.");  //NOLINT
DEFINE_int32(prepare_data, 0, "Number of records populated to the built-in tables. Specify 0 to disable. Requires --builtin_tables.");  //NOLINT

namespace jogasaki::common_cli {

using takatori::util::unsafe_downcast;

void set_flag_defaults(std::vector<std::pair<std::string, std::string>> const& defaults) {
    for(auto&& [name, value] : defaults) {
        if(gflags::SetCommandLineOptionWithMode(name.c_str(), value.c_str(), gflags::SET_FLAGS_DEFAULT).empty()) {
            LOG(ERROR) << "failed to set default value of flag " << name << " to " << value;
        }
    }
}

bool fill_configuration(configuration& cfg) {
    auto assignments = static_cast<int>(FLAGS_core_affinity) +
        static_cast<int>(FLAGS_assign_numa_nodes_uniformly) +
        static_cast<int>(FLAGS_force_numa_node >= 0);
    if(assignments > 1) {
        LOG(ERROR) << "--core_affinity, --assign_numa_nodes_uniformly and --force_numa_node are exclusive with each other";
        return false;
    }
    cfg.single_thread(FLAGS_single_thread);
    cfg.thread_pool_size(FLAGS_thread_count);
    cfg.core_affinity(FLAGS_core_affinity);
    cfg.initial_core(FLAGS_initial_core);
    cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
    if(FLAGS_force_numa_node >= 0) {
        cfg.force_numa_node(FLAGS_force_numa_node);
    }
    cfg.default_partitions(FLAGS_partitions);
    cfg.stealing_enabled(FLAGS_steal);
    cfg.stealing_wait(FLAGS_stealing_wait);
    cfg.task_polling_wait(FLAGS_task_polling_wait);
    cfg.use_preferred_worker_for_current_thread(FLAGS_use_preferred_worker_for_current_thread);
    cfg.enable_hybrid_scheduler(FLAGS_enable_hybrid_scheduler);
    cfg.lightweight_job_level(FLAGS_lightweight_job_level);
    cfg.busy_worker(FLAGS_busy_worker);
    cfg.watcher_interval(FLAGS_watcher_interval);
    cfg.worker_try_count(FLAGS_worker_try_count);
    cfg.worker_suspend_timeout(FLAGS_worker_suspend_timeout);
    cfg.scan_block_size(FLAGS_scan_block_size);
    cfg.scan_yield_interval(FLAGS_scan_yield_interval);
    cfg.scan_default_parallel(FLAGS_scan_default_parallel);
    cfg.max_result_set_writers(FLAGS_max_result_set_writers);
    if (! FLAGS_commit_response.empty()) {
        bool found = false;
        for(auto k : {
            commit_response_kind::accepted,
            commit_response_kind::available,
            commit_response_kind::stored,
            commit_response_kind::propagated,
        }) {
            if (FLAGS_commit_response == to_string_view(k)) {
                cfg.default_commit_response(k);
                found = true;
            }
        }
        if (! found) {
            LOG(ERROR) << "invalid --commit_response: " << FLAGS_commit_response;
            return false;
        }
    }
    if((FLAGS_prepare_data > 0 || ! FLAGS_load_from.empty()) && ! FLAGS_builtin_tables) {
        LOG(ERROR) << "--prepare_data and --load_from require --builtin_tables";
        return false;
    }
    return true;
}

service_environment::~service_environment() {
    stop();
}

bool service_environment::start(std::shared_ptr<configuration> cfg) {
    if (FLAGS_location == "TMP") {
        temporary_.prepare();
        cfg->db_location(temporary_.path());
    } else {
        cfg->db_location(std::string(FLAGS_location));
    }
    cfg->skip_smv_check(true);  // skip strict version check for internal use
    if (FLAGS_epoch_duration > 0) {
        // jogasaki opens kvs only with the location, so open it here to pass other options
        sharksfin::DatabaseOptions options{};
        options.attribute("location", cfg->db_location());
        options.attribute("epoch_duration", std::to_string(FLAGS_epoch_duration));
        if (auto rc = sharksfin::database_open(options, nullptr, &kvs_); rc != sharksfin::StatusCode::OK) {
            LOG(ERROR) << "failed to open kvs: " << rc;
            kvs_ = nullptr;
            temporary_.clean();
            return false;
        }
        db_ = std::shared_ptr{api::create_database(cfg, kvs_)};
    } else {
        db_ = std::shared_ptr{api::create_database(cfg)};
    }
    auto c = std::make_shared<tateyama::api::configuration::whole>("");
    service_ = std::make_shared<api::impl::service>(c, db_.get());
    if (auto rc = db_->start(); rc != status::ok) {
        LOG(ERROR) << "failed to start database: " << rc;
        db_.reset();
        service_.reset();
        close_kvs();
        temporary_.clean();
        return false;
    }
    started_ = true;
    return true;
}

void service_environment::stop() {
    if (started_) {
        db_->stop();
        started_ = false;
    }
    service_.reset();
    db_.reset();
    close_kvs();
    temporary_.clean();
}

void service_environment::close_kvs() {
    if (kvs_ == nullptr) {
        return;
    }
    // the database borrows the kvs handle, so the owner closes it
    if (auto rc = sharksfin::database_close(kvs_); rc != sharksfin::StatusCode::OK) {
        LOG(ERROR) << "failed to close kvs: " << rc;
    }
    sharksfin::database_dispose(kvs_);
    kvs_ = nullptr;
}

bool setup_builtin_tables(api::database& db) {
    if (! FLAGS_builtin_tables) {
        return true;
    }
    utils::add_test_tables();
    utils::add_benchmark_tables();
    if (! FLAGS_load_from.empty()) {
        load(db, FLAGS_load_from);
    }
    if (FLAGS_prepare_data > 0) {
        auto& db_impl = unsafe_downcast<api::impl::database&>(db);
        for(auto&& name : {
            "T0", "T1", "T2",
            "WAREHOUSE", "DISTRICT", "CUSTOMER", "NEW_ORDER", "ORDERS", "ORDER_LINE", "ITEM", "STOCK", "HISTORY"
        }) {
            utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), name, FLAGS_prepare_data, true, prepared_data_modulo);
        }
    }
    return true;
}

}  // namespace jogasaki::common_cli
