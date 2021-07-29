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
#include <chrono>
#include <future>

#include <glog/logging.h>
#include <takatori/util/fail.h>

#include <jogasaki/api.h>
#include <jogasaki/api/environment.h>
#include <jogasaki/api/result_set.h>
#include "utils.h"

DEFINE_bool(single_thread, false, "Whether to run on serial scheduler");  //NOLINT
DEFINE_bool(work_sharing, false, "Whether to use on work sharing scheduler when run parallel");  //NOLINT
DEFINE_int64(duration, 5000, "Run duration in milli-seconds");  //NOLINT
DEFINE_int64(queries, -1, "Number of queries per client thread. Specify -1 to use duration instead.");  //NOLINT
DEFINE_int32(thread_count, 10, "Number of threads");  //NOLINT
DEFINE_int32(clients, 10, "Number of client threads");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_int32(partitions, 10, "Number of partitions per process");  //NOLINT
DEFINE_bool(steal, false, "Enable stealing for task scheduling");  //NOLINT

namespace jogasaki::query_bench_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;

using takatori::util::fail;

using clock = std::chrono::high_resolution_clock;

static bool prepare_data(api::database& db) {
    std::string insert_warehouse{"INSERT INTO WAREHOUSE (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (1, 'fogereb', 'byqosjahzgrvmmmpglb', 'kezsiaxnywrh', 'jisagjxblbmp', 'ps', '694764299', 0.12, 3000000.00)"};
    std::string insert_customer{ "INSERT INTO CUSTOMER (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data, c_ytd_payment, c_payment_cnt, c_delivery_cnt)  VALUES (1, 1, 1, 'pmdeqxrbgs', 'OE', 'BARBARBAR', 'zlaoknusaxfhasce', 'sagjvpdsyzbhsvnhwzxe', 'adftkgtros', 'qd', '827402212', '8700969702524002', '1973-12-12', 'BC', 50000.00, 0.05, -9.99, 'posxrsroejldsyoyirjofkqsycnbjoalxfkgipoogepnuwmagaxcopincpbfhwercrohqxygjjxhamineoraxkzrirkafmmjkcbkafvnqfzonsdcccijdzqlbywgcgbovpmmjcapfmfqbjnfejaqmhqqtxjayvowuujxqmzvisjghpjpynbamdhvvjncvgzstpvqeeakdpwkjmircrfysmwbbbkzbzefldktqfeubcbcjgdjsjtkcomuhqdazqmgpukiyawmqgyzkciwrxfswnegkrofklawoxypehzzztouvokzhshawbbdkasynuixskxmauxuapnkemytcrchqhvjqhntkvkmgezotza', 10.00, 1, 0)"};

    std::unique_ptr<api::executable_statement> p1{};
    std::unique_ptr<api::executable_statement> p2{};
    if(auto rc = db.create_executable(insert_warehouse, p1); rc != status::ok) {
        return false;
    }
    if(auto rc = db.create_executable(insert_customer, p2); rc != status::ok) {
        return false;
    }

    auto tx = db.create_transaction();
    if(auto rc = tx->execute(*p1); rc != status::ok) {
        tx->abort();
        return false;
    }
    if(auto rc = tx->execute(*p2); rc != status::ok) {
        tx->abort();
        return false;
    }
    tx->commit();
    return true;
}

static std::unique_ptr<api::prepared_statement> prepare(api::database& db) {
    std::string select{
        "SELECT w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER "
        "WHERE w_id = :w_id "
        "AND c_w_id = w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id "
    };
    db.register_variable("w_id", api::field_type_kind::int8);
    db.register_variable("c_d_id", api::field_type_kind::int8);
    db.register_variable("c_id", api::field_type_kind::int8);

    std::unique_ptr<api::prepared_statement> p{};
    if(auto rc = db.prepare(select, p); rc != status::ok) {
        std::abort();
    }
    return p;
}

static bool query(api::database& db, api::prepared_statement const& stmt, std::size_t& result) {
    auto ps = api::create_parameter_set();
    ps->set_int8("w_id", 1);
    ps->set_int8("c_d_id", 1);
    ps->set_int8("c_id", 1);

    std::unique_ptr<api::executable_statement> e{};
    if(auto rc = db.resolve(stmt, *ps, e); rc != status::ok) {
        return false;
    }

    auto tx = db.create_transaction();
    std::unique_ptr<api::result_set> rs{};
    if(auto rc = tx->execute(*e, rs); rc != status::ok) {
        return false;
    }

    auto it = rs->iterator();
    while(it->has_next()) {
        auto* record = it->next();
        DVLOG(1) << *record;
        (void)record;
        result += record->get_int8(0);
    }
    tx->commit();
    rs->close();
    return true;
}

bool fill_from_flags(
    configuration& cfg,
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
    cfg.core_affinity(FLAGS_core_affinity);
    cfg.initial_core(FLAGS_initial_core);
    cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
    cfg.thread_pool_size(FLAGS_thread_count);
    cfg.default_partitions(FLAGS_partitions);
    cfg.stealing_enabled(FLAGS_steal);

    if (FLAGS_minimum) {
        cfg.thread_pool_size(1);
        cfg.initial_core(1);
        cfg.core_affinity(false);
        cfg.default_partitions(1);
    }

    if (cfg.assign_numa_nodes_uniformly()) {
        cfg.core_affinity(true);
    }
    return true;
}

void show_result(
    std::int64_t total_executions,
    std::size_t duration_ms,
    std::size_t threads,
    bool debug
) {
    if (debug) {
        LOG(INFO) << "======= begin debug info =======";
    }
    // TODO
    if (debug) {
        LOG(INFO) << "======= end debug info =======";
    }

    LOG(INFO) << "duration: " << format(duration_ms) << " ms";
    LOG(INFO) << "total executions: " << format(total_executions) << " transactions";
    LOG(INFO) << "total throughput: " << format((std::int64_t)((double)total_executions / duration_ms * 1000)) << " transactions/s";
    LOG(INFO) << "avg throughput: " << format((std::int64_t)((double)total_executions / threads / duration_ms * 1000)) << " transactions/s/thread";
}

static int run(
    std::shared_ptr<jogasaki::configuration> cfg,
    bool debug,
    std::size_t duration,
    std::int64_t queries,
    std::size_t clients
) {
//    LOG(INFO) << "configuration " << *cfg;
    auto env = jogasaki::api::create_environment();
    env->initialize();
    cfg->prepare_benchmark_tables(true);
    auto db = jogasaki::api::create_database(cfg);
    db->start();

    if(auto res = prepare_data(*db); !res) {
        db->stop();
        return -1;
    }

    std::vector<std::future<std::int64_t>> results{};
    results.reserve(clients);
    std::atomic_bool stop = false;
    for(std::size_t i=0; i < clients; ++i) {
        results.emplace_back(
            std::async(std::launch::async, [&](){
                std::int64_t count = 0;
                std::size_t result = 0;
                auto stmt = prepare(*db);
                while((queries == -1 && !stop) || (queries != -1 && count < queries)) {
                    if(auto res = query(*db, *stmt, result); !res) {
                        LOG(ERROR) << "query error";
                        std::abort();
                    }
                    ++count;
                }
                if (result == 0) {
                    LOG(INFO) << "client " << i << " output no result";
                }
                return count;
            })
        );
    }
    auto begin = clock::now();
    if (queries == -1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(duration));
        stop = true;
    }
    std::int64_t total_executions{};
    for(auto&& f : results) {
        total_executions += f.get();
    }
    results.clear();
    auto end = clock::now();
    auto duration_ms = std::chrono::duration_cast<clock::duration>(end-begin).count()/1000/1000;
    show_result(total_executions, duration_ms, cfg->thread_pool_size(), debug);
    db->stop();
    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    gflags::SetUsageMessage("query-bench cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1) {
        gflags::ShowUsageWithFlags(argv[0]); // NOLINT
        return -1;
    }
    auto cfg = std::make_shared<jogasaki::configuration>();
    if(! jogasaki::query_bench_cli::fill_from_flags(*cfg)) return -1;
    try {
        jogasaki::query_bench_cli::run(cfg, FLAGS_debug, FLAGS_duration, FLAGS_queries, FLAGS_clients);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
