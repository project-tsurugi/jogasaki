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

#include <executor/common/graph.h>
#include <executor/common/port.h>
#include <scheduler/dag_controller.h>
#include <executor/exchange/deliver/step.h>
#include <executor/exchange/group/shuffle_info.h>
#include "../common/producer_process.h"
#include "cogroup_process.h"

namespace jogasaki::cogroup_cli {

using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::executor::exchange::group;
using namespace jogasaki::scheduler;

DEFINE_int32(thread_pool_size, 5, "Thread pool size");  //NOLINT

std::shared_ptr<meta::record_meta> test_record_meta() {
    return std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                    meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int8>),
                    meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float8>),
            },
            boost::dynamic_bitset<std::uint64_t>{std::string("00")});
}

static int run() {
    auto meta = test_record_meta();
    auto info = std::make_shared<shuffle_info>(meta, std::vector<std::size_t>{0});

    auto g = std::make_unique<common::graph>();
    auto scan1 = std::make_unique<producer_process>(g.get(), meta);
    auto scan2 = std::make_unique<producer_process>(g.get(), meta);
    auto xch1 = std::make_unique<group::step>(info);
    auto xch2 = std::make_unique<group::step>(info);
    auto cgrp = std::make_unique<cogroup_process>();
//    auto emit = std::make_unique<consumer_process>(g.get(), info->group_meta());
    auto dvr = std::make_unique<deliver::step>();
    *scan1 >> *xch1;
    *scan2 >> *xch2;
    *xch1 >> *cgrp;
    *xch2 >> *cgrp;
    *cgrp >> *dvr;
    // step id are assigned from 0 to 5
    g->insert(std::move(scan1));
    g->insert(std::move(xch1));
    g->insert(std::move(scan2));
    g->insert(std::move(xch2));
    g->insert(std::move(cgrp));
    g->insert(std::move(dvr));

    configuration cfg;
    cfg.thread_pool_size = 1;
    cfg.single_thread_task_scheduler = true;
    dag_controller dc{&cfg};
    dc.schedule(*g);
    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("group cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("group cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    try {
        return jogasaki::cogroup_cli::run();  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}

