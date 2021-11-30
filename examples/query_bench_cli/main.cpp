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

#include <boost/thread/latch.hpp>

#include <glog/logging.h>
#include <takatori/util/fail.h>

#include <jogasaki/api.h>
#include <jogasaki/api/environment.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/common.h>
#include <jogasaki/utils/random.h>
#include <jogasaki/utils/create_tx.h>
#include "utils.h"
#include "../common/temporary_folder.h"

#include <tateyama/utils/thread_affinity.h>

DEFINE_bool(single_thread, false, "Whether to run on serial scheduler");  //NOLINT
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
DEFINE_int32(records, 100, "Number of records on the target table");  //NOLINT
DEFINE_int32(client_initial_core, -1, "set the client thread core affinity and assign sequentially from the specified core. Specify -1 not to set core-level thread affinity, then threads are distributed on numa nodes uniformly.");  //NOLINT
DEFINE_bool(readonly, true, "Specify readonly option when creating transaction");  //NOLINT
DEFINE_string(location, "TMP", "specify the database directory. Pass TMP to use temporary directory.");  //NOLINT
DEFINE_bool(simple, false, "use simple query");  //NOLINT

namespace jogasaki::query_bench_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;

using takatori::util::fail;

using clock = std::chrono::high_resolution_clock;

static bool prepare_data(api::database& db, std::size_t records) {
    std::string insert_warehouse_fmt{"INSERT INTO WAREHOUSE (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES (%d, 'fogereb', 'byqosjahzgrvmmmpglb', 'kezsiaxnywrh', 'jisagjxblbmp', 'ps', '694764299', 0.12, 3000000.00)"};
    std::string insert_customer_fmt{ "INSERT INTO CUSTOMER (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data, c_ytd_payment, c_payment_cnt, c_delivery_cnt)  VALUES (%d, %d, %d, 'pmdeqxrbgs', 'OE', 'BARBARBAR', 'zlaoknusaxfhasce', 'sagjvpdsyzbhsvnhwzxe', 'adftkgtros', 'qd', '827402212', '8700969702524002', '1973-12-12', 'BC', 50000.00, 0.05, -9.99, 'posxrsroejldsyoyirjofkqsycnbjoalxfkgipoogepnuwmagaxcopincpbfhwercrohqxygjjxhamineoraxkzrirkafmmjkcbkafvnqfzonsdcccijdzqlbywgcgbovpmmjcapfmfqbjnfejaqmhqqtxjayvowuujxqmzvisjghpjpynbamdhvvjncvgzstpvqeeakdpwkjmircrfysmwbbbkzbzefldktqfeubcbcjgdjsjtkcomuhqdazqmgpukiyawmqgyzkciwrxfswnegkrofklawoxypehzzztouvokzhshawbbdkasynuixskxmauxuapnkemytcrchqhvjqhntkvkmgezotza', 10.00, 1, 0)"};
    std::string insert_district_fmt{ "INSERT INTO DISTRICT (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES (%d, %d, 'fvcclfvyp', 'lopauzeyaipx', 'uwnikzbvcj', 'pxsfqptmnwm', 'yn', '393838416', 0.18, 30000.00, 3001)"};
    for(std::size_t i=0; i < records; ++i) {
        auto insert_warehouse = format(insert_warehouse_fmt, i);
        auto insert_customer = format(insert_customer_fmt, i, i, i);
        auto insert_district = format(insert_district_fmt, i, i);
        std::unique_ptr<api::executable_statement> p1{};
        std::unique_ptr<api::executable_statement> p2{};
        std::unique_ptr<api::executable_statement> p3{};
        if(auto rc = db.create_executable(insert_warehouse, p1); rc != status::ok) {
            return false;
        }
        if(auto rc = db.create_executable(insert_customer, p2); rc != status::ok) {
            return false;
        }
        if(auto rc = db.create_executable(insert_district, p3); rc != status::ok) {
            return false;
        }

        auto tx = utils::create_transaction(db);
        if(auto rc = tx->execute(*p1); rc != status::ok) {
            tx->abort();
            return false;
        }
        if(auto rc = tx->execute(*p2); rc != status::ok) {
            tx->abort();
            return false;
        }
        if(auto rc = tx->execute(*p3); rc != status::ok) {
            tx->abort();
            return false;
        }
        tx->commit();
    }
    return true;
}

static api::statement_handle prepare(api::database& db) {
    std::string select{
        "SELECT w_id, w_tax, c_discount, c_last, c_credit FROM WAREHOUSE, CUSTOMER "
        "WHERE w_id = :w_id "
        "AND c_w_id = w_id AND "
        "c_d_id = :c_d_id AND "
        "c_id = :c_id "
    };
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"w_id", api::field_type_kind::int8},
        {"c_d_id", api::field_type_kind::int8},
        {"c_id", api::field_type_kind::int8},
    };
    api::statement_handle p{};
    if(auto rc = db.prepare(select, variables, p); rc != status::ok) {
        std::abort();
    }
    return p;
}

static api::statement_handle prepare_simple(api::database& db) {
    std::string select{
        "SELECT d_next_o_id, d_tax FROM DISTRICT "
        "WHERE "
        "d_w_id = :d_w_id AND "
        "d_id = :d_id "
    };
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"d_w_id", api::field_type_kind::int8},
        {"d_id", api::field_type_kind::int8},
    };
    api::statement_handle p{};
    if(auto rc = db.prepare(select, variables, p); rc != status::ok) {
        std::abort();
    }
    return p;
}

static bool query(
    api::database& db,
    api::statement_handle& stmt,
    jogasaki::utils::xorshift_random32& rnd,
    std::size_t records,
    bool readonly,
    bool simple,
    std::size_t& result
) {
    auto ps = api::create_parameter_set();
    auto id = rnd() % records;
    if (simple) {
        ps->set_int8("d_w_id", id);
        ps->set_int8("d_id", id);
    } else {
        ps->set_int8("w_id", id);
        ps->set_int8("c_d_id", id);
        ps->set_int8("c_id", id);
    }

    std::unique_ptr<api::executable_statement> e{};
    {
        trace_scope_name("resolve");  //NOLINT
        if(auto rc = db.resolve(stmt, std::shared_ptr{std::move(ps)}, e); rc != status::ok) {
            return false;
        }
    }

    auto tx = utils::create_transaction(db, readonly);
    std::unique_ptr<api::result_set> rs{};
    {
        trace_scope_name("execute");  //NOLINT
        if(auto rc = tx->execute(*e, rs); rc != status::ok) {
            return false;
        }
    }
    {
        trace_scope_name("iterate");  //NOLINT
        auto it = rs->iterator();
        while(it->has_next()) {
            auto* record = it->next();
            DVLOG(1) << *record;
            (void)record;
            result += record->get_int8(0);
        }
    }
    {
        trace_scope_name("commit");  //NOLINT
        tx->commit();
    }
    {
        trace_scope_name("rs_close");  //NOLINT
        rs->close();
    }
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
    std::size_t threads
) {
    LOG(INFO) << "duration: " << format(duration_ms) << " ms";
    LOG(INFO) << "total executions: " << format(total_executions) << " transactions";
    LOG(INFO) << "total throughput: " << format((std::int64_t)((double)total_executions / duration_ms * 1000)) << " transactions/s";
    LOG(INFO) << "avg throughput: " << format((std::int64_t)((double)total_executions / threads / duration_ms * 1000)) << " transactions/s/thread";
}

static int run(
    std::shared_ptr<jogasaki::configuration> cfg,
    bool debug,
    bool simple,
    std::size_t duration,
    std::int64_t queries,
    std::size_t clients,
    std::size_t records
) {
    auto env = jogasaki::api::create_environment();
    cfg->prepare_benchmark_tables(true);
    env->initialize(); //init before logging
    LOG(INFO) << "configuration " << *cfg
        << "debug:" << debug << " "
        << "simple:" << simple << " "
        << "duration:" << duration << " "
        << "queries:" << queries << " "
        << "clients:" << clients<< " "
        << "";

    jogasaki::common_cli::temporary_folder dir{};
    if (FLAGS_location == "TMP") {
        dir.prepare();
        cfg->db_location(dir.path());
    } else {
        cfg->db_location(std::string(FLAGS_location));
    }
    auto db = jogasaki::api::create_database(cfg);
    db->start();

    if(auto res = prepare_data(*db, records); !res) {
        LOG(ERROR) << "prepare dat error";
        db->stop();
        return -1;
    }

    std::vector<std::future<std::int64_t>> results{};
    results.reserve(clients);
    std::atomic_bool stop = false;
    boost::latch start{clients};
    for(std::size_t i=0; i < clients; ++i) {
        results.emplace_back(
            std::async(std::launch::async, [&, i](){
                if (FLAGS_client_initial_core != -1) {
                    tateyama::utils::set_thread_affinity(i,
                        {
                            tateyama::utils::affinity_tag<tateyama::utils::affinity_kind::core_affinity>,
                            static_cast<std::size_t>(FLAGS_client_initial_core)
                        }
                    );
                } else {
                    // by default assign the on numa nodes uniformly
                    tateyama::utils::set_thread_affinity(i, tateyama::utils::affinity_profile{
                        tateyama::utils::affinity_tag<tateyama::utils::affinity_kind::numa_affinity>
                    });
                }
                std::int64_t count = 0;
                std::size_t result = 0;
                auto stmt = simple ? prepare_simple(*db) : prepare(*db);
                start.count_down_and_wait();
                jogasaki::utils::xorshift_random32 rnd{static_cast<std::uint32_t>(123456+i)};
                while((queries == -1 && !stop) || (queries != -1 && count < queries)) {
                    if(auto res = query(*db, stmt, rnd, records, FLAGS_readonly, simple, result); !res) {
                        LOG(ERROR) << "query error";
                        std::abort();
                    }
                    ++count;
                }
                if (result == 0) {
                    LOG(INFO) << "client " << i << " output no result";
                }
                db->destroy_statement(stmt);
                return count;
            })
        );
    }
    start.wait();
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
    show_result(total_executions, duration_ms, cfg->thread_pool_size());
    db->stop();
    dir.clean();
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
    auto queries = FLAGS_queries;
    auto clients = FLAGS_clients;
    if (FLAGS_minimum) {
        queries = 5;
        clients = 1;
    }
    try {
        jogasaki::query_bench_cli::run(
            cfg,
            FLAGS_debug,
            FLAGS_simple,
            FLAGS_duration,
            queries,
            clients,
            FLAGS_records
        );  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
