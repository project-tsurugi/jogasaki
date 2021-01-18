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

#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/result_set.h>

namespace jogasaki::client_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using kind = meta::field_type_kind;
using takatori::util::fail;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::expression;
using takatori::util::enum_tag_t;
using takatori::util::enum_tag;

static int run(std::string_view sql) {
    if (sql.empty()) return 0;
    auto db = jogasaki::api::create_database();
    db->start();

//    auto db_impl = api::database::impl::get_impl(db);
//    executor::add_benchmark_tables(*db_impl->tables());
//    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I0", 10, true, 5);
//    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I1", 10, true, 5);
//    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "I2", 10, true, 5);

//    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "WAREHOUSE0", 10, true, 5);
//    utils::populate_storage_data(db_impl->kvs_db().get(), db_impl->tables(), "CUSTOMER0", 10, true, 5);

    std::unique_ptr<api::result_set> rs{};
    if(auto res = db->execute(sql, rs); !res || !rs) {
        db->stop();
        return 0;
    }
    auto it = rs->begin();
    while(it != rs->end()) {
        auto record = it.ref();
        std::stringstream ss{};
        ss << record << *rs->meta();
        LOG(INFO) << ss.str();
        ++it;
    }
    rs->close();
    db->stop();
    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("client cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("client cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 2) {
        gflags::ShowUsageWithFlags(argv[0]); // NOLINT
        return -1;
    }
    std::string_view source { argv[1] }; // NOLINT
    try {
        jogasaki::client_cli::run(source);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
