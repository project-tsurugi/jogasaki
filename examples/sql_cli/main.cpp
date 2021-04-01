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

namespace jogasaki::sql_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using kind = meta::field_type_kind;
using takatori::util::fail;
using takatori::util::unsafe_downcast;
using yugawara::storage::configurable_provider;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::expression;
using takatori::util::enum_tag_t;
using takatori::util::enum_tag;

constexpr kvs::order asc = kvs::order::ascending;
constexpr kvs::order desc = kvs::order::descending;
constexpr kvs::order undef = kvs::order::undefined;

static int run(std::string_view sql) {
    if (sql.empty()) return 0;
    auto db = api::create_database();
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
    std::string_view source { argv[1] }; // NOLINT
    try {
        jogasaki::sql_cli::run(source);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
