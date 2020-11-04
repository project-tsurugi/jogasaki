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

#include <jogasaki/api/database.h>
#include <jogasaki/api/result_set.h>

namespace jogasaki::sql_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;

static int run(std::string_view sql) {
    if (sql.empty()) return 0;
    jogasaki::api::database db{};
    db.start();
    auto rs = db.execute(sql);
    auto it = rs->begin();
    while(it != rs->end()) {
        auto record = *it;
        LOG(INFO) << "C0: " << record.get_value<std::int64_t>(0) << " C1: " << record.get_value<double>(8); //FIXME use record_meta
        ++it;
    }
    rs->close();
    db.stop();
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
