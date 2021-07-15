/*
 * Copyright 2018-2019 tsurugi project.
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
#include <iostream>
#include <vector>

#include <glog/logging.h>
#include <takatori/util/fail.h>
#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/executor/tables.h>

#include "../common/load.h"

DEFINE_bool(single_thread, false, "Whether to run on serial scheduler");  //NOLINT
DEFINE_int32(thread_count, 10, "Number of threads");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT

namespace jogasaki::sql_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using kind = meta::field_type_kind;
using takatori::util::unsafe_downcast;
using yugawara::storage::configurable_provider;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::expression;

static int run(std::string_view sql, std::shared_ptr<configuration> cfg) {
    if (sql.empty()) return 0;
    auto db = api::create_database(cfg);
    db->start();
    auto db_impl = unsafe_downcast<api::impl::database>(db.get());
    executor::add_benchmark_tables(*db_impl->tables());
    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I0", 10, true, 5);
    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I1", 10, true, 5);
    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I2", 10, true, 5);
    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "WAREHOUSE", 10, true, 5);
    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "CUSTOMER", 10, true, 5);

    std::unique_ptr<api::executable_statement> e{};
    if(auto rc = db->create_executable(sql, e); rc != status::ok) {
        db->stop();
        return -1;
    }
    std::unique_ptr<api::result_set> rs{};
    {
        auto tx = db->create_transaction();
        if(auto rc = tx->execute(*e, rs); rc != status::ok || !rs) {
            db->stop();
            return -1;
        }
        auto it = rs->iterator();
        while(it->has_next()) {
            auto* record = it->next();
            std::stringstream ss{};
            ss << *record;
            LOG(INFO) << ss.str();
        }
        rs->close();
        tx->commit();
    }
    db->stop();
    return 0;
}

bool fill_from_flags(
    jogasaki::configuration& cfg,
    std::string const& str = {}
) {
    gflags::FlagSaver saver{};
    if (! str.empty()) {
        if(! gflags::ReadFlagsFromString(str, "", false)) {
            std::cerr << "parsing options failed" << std::endl;
        }
    }
    cfg.single_thread(FLAGS_single_thread);
    cfg.thread_pool_size(FLAGS_thread_count);

    cfg.core_affinity(FLAGS_core_affinity);
    cfg.initial_core(FLAGS_initial_core);
    cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);

    if (FLAGS_minimum) {
        cfg.single_thread(true);
        cfg.thread_pool_size(1);
        cfg.initial_core(1);
        cfg.core_affinity(false);
    }

    if (cfg.assign_numa_nodes_uniformly()) {
        cfg.core_affinity(true);
    }
    return true;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("sql cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("sql cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlags(argv[0]); // NOLINT
        return -1;
    }
    auto cfg = std::make_shared<jogasaki::configuration>();
    jogasaki::sql_cli::fill_from_flags(*cfg);
    std::string_view source { argv[1] }; // NOLINT
    try {
        jogasaki::sql_cli::run(source, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
