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
#include <jogasaki/executor/sequence/sequence.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/api.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/executor/tables.h>
#include <tateyama/common.h>
#include <jogasaki/scheduler/task_scheduler.h>

#include "../common/load.h"
#include "../common/temporary_folder.h"
#include <jogasaki/utils/create_tx.h>

DEFINE_bool(single_thread, false, "Whether to run on serial scheduler");  //NOLINT
DEFINE_bool(work_sharing, false, "Whether to use on work sharing scheduler when run parallel");  //NOLINT
DEFINE_int32(thread_count, 1, "Number of threads");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(explain, false, "explain the execution plan");  //NOLINT
DEFINE_int32(partitions, 10, "Number of partitions per process");  //NOLINT
DEFINE_bool(steal, false, "Enable stealing for task scheduling");  //NOLINT
DEFINE_bool(auto_commit, true, "Whether to commit when finishing each statement.");  //NOLINT
DEFINE_bool(prepare_data, false, "Whether to prepare a few records in the storages");  //NOLINT
DEFINE_string(location, "", "specify the database directory. Pass TMP to use temporary directory.");  //NOLINT
DEFINE_bool(async, false, "Whether to use new async api");  //NOLINT

namespace jogasaki::sql_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using kind = meta::field_type_kind;
using takatori::util::unsafe_downcast;
using yugawara::storage::configurable_provider;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::expression;

class cli {
    std::unique_ptr<api::database> db_{};
    std::shared_ptr<api::transaction_handle> tx_{};
    common_cli::temporary_folder temporary_{};
public:
    int end_tx(bool abort = false) {
        if (abort) {
            if(auto rc = tx_->abort(); rc != status::ok) {
                std::abort();
            }
            return 0;
        }
        if(auto rc = tx_->commit(); rc != status::ok) {
            LOG(ERROR) << "commit: " << rc;
            return static_cast<int>(rc);
        }
        return 0;
    }
    int execute_stmt(
        std::string_view stmt
    ) {
        if (stmt.empty()) return 0;
        std::unique_ptr<api::executable_statement> e{};
        if(auto rc = db_->create_executable(stmt, e); rc != status::ok) {
            LOG(ERROR) << rc;
            return -1;
        }
        if (FLAGS_explain) {
            db_->explain(*e, std::cout);
            std::cout << std::endl;
            return 1;
        }
        if (FLAGS_async) {
            std::atomic_bool run{false};
            api::test_channel ch{};
            if(auto rc = tx_->execute_async(
                std::shared_ptr{std::move(e)},
                maybe_shared_ptr{&ch},
                    [&](status st, std::string_view msg){
                        LOG(INFO) << "completed status:" << st << " msg:" << msg;
                        run = true;
                    }
                ); !rc) {
                LOG(ERROR) << rc;
                return -1;
            }
            while(! run.load()) {}
        } else {
            std::unique_ptr<api::result_set> rs{};
            if(auto rc = tx_->execute(*e, rs); rc != status::ok || !rs) {
                LOG(ERROR) << rc;
                return static_cast<int>(rc);
            }
            auto it = rs->iterator();
            while(it->has_next()) {
                auto* record = it->next();
                std::stringstream ss{};
                ss << *record;
                LOG(INFO) << ss.str();
            }
            rs->close();
        }
        return 0;
    }

    void prepare_data(api::database& db) {
        auto& db_impl = unsafe_downcast<api::impl::database&>(db);
        executor::add_benchmark_tables(*db_impl.tables());
        utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "T0", 10, true, 5);
        utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "T1", 10, true, 5);
        utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "T2", 10, true, 5);
        utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "WAREHOUSE", 10, true, 5);
        utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "CUSTOMER", 10, true, 5);
    }

    std::vector<std::string> split(std::string_view sql) {
        std::vector<std::string> stmts{};
        std::stringstream ss{std::string(sql)};
        std::string buf;
        while (std::getline(ss, buf, ';')) {
            stmts.push_back(buf);
        }
        return stmts;
    }

    int execute_statements(std::vector<std::string> const& stmts, bool auto_commit) {
        if (FLAGS_prepare_data) {
            prepare_data(*db_);
        }
        tx_ = utils::create_transaction(*db_);
        int rc{};
        for(auto&& s: stmts) {
            rc = execute_stmt(s);
            if (rc < 0) {
                end_tx(true);
                return rc;
            }
            if (auto_commit) {
                if (auto rc2 = end_tx()) {
                    return rc2;
                }
                tx_ = utils::create_transaction(*db_);
            }
        }
        end_tx();
        return rc;
    }
    int run(std::string_view sql, std::shared_ptr<configuration> cfg) {
        trace_scope;
        if (sql.empty()) return 0;
        auto stmts = split(sql);
        db_ = api::create_database(cfg);
        db_->start();
        auto rc = execute_statements(stmts, FLAGS_auto_commit);
        db_->stop();
        if (rc) {
            LOG(ERROR) << "exit code: " << rc;
        }
        temporary_.clean();
        return rc;
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
        cfg.work_sharing(FLAGS_work_sharing);
        cfg.thread_pool_size(FLAGS_thread_count);

        cfg.core_affinity(FLAGS_core_affinity);
        cfg.initial_core(FLAGS_initial_core);
        cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
        cfg.default_partitions(FLAGS_partitions);
        cfg.stealing_enabled(FLAGS_steal);

        if (FLAGS_minimum) {
            cfg.single_thread(true);
            cfg.thread_pool_size(1);
            cfg.initial_core(1);
            cfg.core_affinity(false);
            cfg.default_partitions(1);
        }

        if (cfg.assign_numa_nodes_uniformly()) {
            cfg.core_affinity(true);
        }
        if (FLAGS_location == "TMP") {
            temporary_.prepare();
            cfg.db_location(temporary_.path());
        } else {
            cfg.db_location(std::string(FLAGS_location));
        }
        return true;
    }
};

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
    jogasaki::sql_cli::cli e{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    e.fill_from_flags(*cfg);
    std::string_view source { argv[1] }; // NOLINT
    try {
        e.run(source, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
