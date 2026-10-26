/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <atomic>
#include <boost/thread/latch.hpp>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cxxabi.h>
#include <emmintrin.h>
#include <exception>
#include <functional>
#include <future>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <takatori/util/downcast.h>
#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <tateyama/api/configuration.h>
#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/proto/diagnostics.pb.h>
#include <tateyama/utils/thread_affinity.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/logging.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/utils/add_test_tables.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/random.h>
#include <jogasaki/utils/runner.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/tables.h>

#include "../query_bench_cli/utils.h"
#include "service_client.h"
#include "service_setup.h"

DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(verify, false, "Whether to deserialize the query result records. Requires clients=1");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(insert, false, "run on insert mode");  //NOLINT
DEFINE_bool(upsert, false, "run on upsert mode");  //NOLINT
DEFINE_bool(update, false, "run on update mode");  //NOLINT
DEFINE_bool(query, false, "run on query mode (point query)");  //NOLINT
DEFINE_bool(query2, false, "run on query mode with multiple records");  //NOLINT
DEFINE_int32(statements, 1000, "The number of statements issued per transaction.");  //NOLINT
DEFINE_int64(duration, 5000, "Run duration in milli-seconds");  //NOLINT
DEFINE_int64(transactions, -1, "Number of transactions executed per client thread. Specify -1 to use duration instead.");  //NOLINT
DEFINE_int32(clients, 1, "Number of client threads");  //NOLINT
DEFINE_int32(client_initial_core, -1, "set the client thread core affinity and assign sequentially from the specified core. Specify -1 not to set core-level thread affinity, then threads are distributed on numa nodes uniformly.");  //NOLINT
DEFINE_bool(ltx, false, "use ltx instead of occ for benchmark. Use exclusively with --rtx.");  //NOLINT
DEFINE_bool(rtx, false, "use rtx instead of occ for benchmark. Use exclusively with --ltx.");  //NOLINT
DEFINE_int64(client_idle, 0, "clients take idle spin loop n times");  //NOLINT
DEFINE_bool(md, false, "output result to stdout as markdown table");  //NOLINT
DEFINE_bool(ddl, false, "issue ddl instead of using built-in table. Required for --secondary.");  //NOLINT
DEFINE_bool(secondary, false, "use secondary index");  //NOLINT
DECLARE_bool(builtin_tables);  //NOLINT
DECLARE_int32(prepare_data);  //NOLINT
DECLARE_int32(thread_count);  //NOLINT

namespace tateyama::service_benchmark {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;
using namespace jogasaki::query_bench_cli;

using takatori::util::unsafe_downcast;
using takatori::util::throw_exception;

using clock = std::chrono::high_resolution_clock;
namespace sql = jogasaki::proto::sql;
using ValueCase = sql::request::Parameter::ValueCase;

struct result_info {
    std::int64_t transactions_{};
    std::int64_t statements_{};
    std::int64_t records_{};
    std::int64_t begin_ns_{};
    std::int64_t statement_ns_{};
    std::int64_t commit_ns_{};
};

enum class mode {
    undefined, insert, update, query, query2, upsert
};

[[nodiscard]] constexpr inline std::string_view to_string_view(mode value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case mode::undefined: return "undefined"sv;
        case mode::insert: return "insert"sv;
        case mode::update: return "update"sv;
        case mode::query: return "query"sv;
        case mode::query2: return "query2"sv;
        case mode::upsert: return "upsert"sv;
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, mode value) {
    return out << to_string_view(value);
}

struct formatted_result {
    std::string duration_;
    std::string avg_begin_commit_interval_;
    std::string avg_statements_interval_;
    std::string executed_transactions_;
    std::string executed_statements_;
    std::string throughput_transactions_;
    std::string throughput_statements_;
    std::string throughput_transactions_per_thread_;
    std::string throughput_statements_per_thread_;
    std::string avg_turn_around_transaction_;
    std::string avg_turn_around_statement_;
    std::string records_;
};

[[nodiscard]] formatted_result create_format_result(
    result_info result,
    std::size_t duration_ms,
    std::size_t threads
) {
    auto transaction_ns = result.begin_ns_ + result.statement_ns_ + result.commit_ns_;
    auto& statement_ns = result.statement_ns_;

    formatted_result ret{};
    auto& transactions = result.transactions_;
    auto& statements = result.statements_;

    ret.duration_ =  format(duration_ms);
    ret.avg_begin_commit_interval_ = format(transactions == 0 ? 0 : (std::int64_t)((double)transaction_ns / transactions));
    ret.avg_statements_interval_ = format(statements == 0 ? 0 : (std::int64_t)((double)statement_ns / statements));
    ret.records_ = format(result.records_);
    ret.executed_transactions_ = format(transactions);
    ret.executed_statements_ = format(statements);
    ret.throughput_transactions_ = format((std::int64_t)((double)transactions / duration_ms * 1000));
    ret.throughput_statements_ = format((std::int64_t)((double) statements / duration_ms * 1000));
    ret.throughput_transactions_per_thread_ = format((std::int64_t)((double)transactions / threads / duration_ms * 1000));
    ret.throughput_statements_per_thread_ = format((std::int64_t)((double)statements / threads / duration_ms * 1000));
    ret.avg_turn_around_transaction_= format((std::int64_t)((double)duration_ms * 1000 * 1000 * threads / transactions));
    ret.avg_turn_around_statement_ = format((std::int64_t)((double) duration_ms * 1000 * 1000 * threads / statements));

    return ret;
}

void display_text(formatted_result const& result) {
    LOG(INFO) << "duration: " << result.duration_ << " ms";
    LOG(INFO) << "  avg. begin-commit interval : " << result.avg_begin_commit_interval_ << " ns/transaction";
    LOG(INFO) << "  avg. statements interval : " << result.avg_statements_interval_ << " ns/statement";
    LOG(INFO) << "executed: " <<
      result.executed_transactions_ << " transactions, " <<
      result.executed_statements_ << " statements, " <<
      result.records_ << " query result writes";
    LOG(INFO) << "throughput: " <<
        result.throughput_transactions_ << " transactions/s, " <<  //NOLINT
        result.throughput_statements_ << " statements/s";  //NOLINT
    LOG(INFO) << "throughput/thread: " <<
        result.throughput_transactions_per_thread_ << " transactions/s/thread, " <<  //NOLINT
        result.throughput_statements_per_thread_ << " statements/s/thread"; //NOLINT
    LOG(INFO) << "avg turn-around: " <<
        "transaction " << result.avg_turn_around_transaction_ << " ns, " <<  //NOLINT
        "statement " << result.avg_turn_around_statement_ << " ns";  //NOLINT
}


void display_md(formatted_result const& result) {
    std::cout << "|";
    std::cout << "stmt|";
    std::cout << "tx type|";
    std::cout << "duration(ms)|";
    std::cout << "threads|";
    std::cout << "clients|";
    std::cout << "statements/tx|";
    // std::cout << "avg. tx interval(ns)|";
    // std::cout << "avg. stmt interval(ns)|";
    // std::cout << "executed txs|";
    std::cout << "executed stmts|";
    // std::cout << "throughput(txs/s)|";
    // std::cout << "throughput(stmts/s)|";
    // std::cout << "throughput(txs/s/thread)|";
    std::cout << "throughput(stmts/s/thread)|";
    // std::cout << "avg turn-around txs(ns)|";
    // std::cout << "avg turn-around stmts(ns)|";

    std::cout << std::endl;
    std::cout << "|-|-|-|-|-|-|-|-|" << std::endl;
    std::cout << "|";
    std::cout << (FLAGS_insert ? "INSERT" : (FLAGS_upsert ? "UPSERT" : (FLAGS_update ? "UPDATE" : (FLAGS_query ? "QUERY" : (FLAGS_query2 ? "QUERY2" : "NA"))))) << "|";
    std::cout << (FLAGS_ltx ? "LTX" : (FLAGS_rtx ? "RTX" : "OCC")) << "|";
    std::cout << result.duration_ << "|";
    std::cout << FLAGS_thread_count << "|";
    std::cout << FLAGS_clients << "|";
    std::cout << FLAGS_statements << "|";
    // std::cout << result.avg_begin_commit_interval_ << "|";
    // std::cout << result.avg_statements_interval_ << "|";
    // std::cout << result.executed_transactions_ << "|";
    std::cout << result.executed_statements_ << "|";
    // std::cout << result.throughput_transactions_ << "|";
    // std::cout << result.throughput_statements_ << "|";
    // std::cout << result.throughput_transactions_per_thread_ << "|";
    std::cout << result.throughput_statements_per_thread_ << "|";
    // std::cout << result.avg_turn_around_transaction_ << "|";
    // std::cout << result.avg_turn_around_statement_ << "|";
    std::cout << std::endl;
}
void show_result(
    result_info result,
    std::size_t duration_ms,
    std::size_t threads,
    bool md
) {
    auto res = create_format_result(result, duration_ms, threads);
    if(! md) {
        display_text(res);
    } else {
        display_md(res);
    }
}

enum class profile {
    tiny,
    normal,
};

template <profile P>
struct profile_t {
    explicit profile_t() = default;
};

template <profile P>
constexpr inline profile_t<P> profile_v{};

struct data_profile {
    data_profile(profile_t<profile::normal>) :  //NOLINT
        new_order_min_(2101),
        new_order_max_(3001),
        stock_item_id_min_(1),
        stock_item_id_max_(100001),
        district_id_min_(1),
        district_id_max_(11)
    {}

    data_profile(profile_t<profile::tiny>) :  //NOLINT
        new_order_min_(22),
        new_order_max_(31),
        stock_item_id_min_(1),
        stock_item_id_max_(51),
        district_id_min_(1),
        district_id_max_(3)
    {}

    std::int64_t new_order_min_{};  //NOLINT
    std::int64_t new_order_max_{};  //exclusive //NOLINT
    std::int64_t stock_item_id_min_{};  //NOLINT
    std::int64_t stock_item_id_max_{}; //exclusive   //NOLINT
    std::int64_t district_id_min_{};  //NOLINT
    std::int64_t district_id_max_{}; //exclusive   //NOLINT
};

class cli {
    jogasaki::common_cli::service_environment env_{};
    bool debug_{}; //NOLINT
    bool verify_query_records_{};
    std::mutex write_buffer_mutex_{};
    std::stringstream write_buffer_{};
    jogasaki::meta::record_meta query_meta_{};

    mode mode_{mode::undefined};
    data_profile profile_{profile_v<profile::normal>};
    std::int64_t transactions_{};
    std::int64_t duration_{};
    std::int64_t statements_{};
    std::size_t clients_{};
    bool ltx_{}; //NOLINT
    bool rtx_{}; //NOLINT
    std::int64_t client_idle_{};
    bool md_{}; //NOLINT
    bool ddl_{}; //NOLINT
    std::size_t secondary_index_count_{}; //NOLINT

public:
    void print_diagnostics() {
        env_.database().print_diagnostic(std::cerr);
    }

    bool fill_from_flags(jogasaki::configuration& cfg) {
        if (FLAGS_minimum) {
            jogasaki::common_cli::set_flag_defaults({
                {"single_thread", "false"},
                {"thread_count", "1"},
                {"initial_core", "1"},
                {"core_affinity", "false"},
                {"assign_numa_nodes_uniformly", "false"},
                {"partitions", "1"},
            });
        }
        // built-in benchmark tables are used unless tables are created by DDL
        FLAGS_builtin_tables = ! FLAGS_ddl;
        if (! jogasaki::common_cli::fill_configuration(cfg)) {
            return false;
        }

        debug_ = FLAGS_debug;
        verify_query_records_ = FLAGS_verify;
        transactions_ = FLAGS_transactions;
        duration_ = FLAGS_duration;
        statements_ = FLAGS_statements;
        clients_ = FLAGS_clients;
        ltx_ = FLAGS_ltx;
        rtx_ = FLAGS_rtx;
        client_idle_ = FLAGS_client_idle;
        md_ = FLAGS_md;
        ddl_ = FLAGS_ddl;
        secondary_index_count_ = FLAGS_secondary ? 1 : 0;
        if (secondary_index_count_ > 0 && ! ddl_) {
            LOG(ERROR) << "secondary index requires --ddl";
            return false;
        }

        if (verify_query_records_ && clients_ != 1) {
            LOG(ERROR) << "--verify requires --clients=1";
            return false;
        }

        if (FLAGS_update) {
            mode_ = mode::update;
        }
        if (FLAGS_query) {
            mode_ = mode::query;
        }
        if (FLAGS_query2) {
            mode_ = mode::query2;
        }
        if (FLAGS_insert) {
            mode_ = mode::insert;
        }
        if (FLAGS_upsert) {
            mode_ = mode::upsert;
        }
        if (ltx_ && rtx_) {
            LOG(ERROR) << "Both --ltx and --rtx are specified.";
            return false;
        }
        if (FLAGS_minimum) {
            mode_ = mode::insert;
            duration_ = -1;
            transactions_ = 1;
            statements_ = 1;
            clients_ = 1;
        }
        if (mode_ == mode::undefined) {
            LOG(ERROR) << "Specify one of --insert/--update/--query/--upsert options.";
            return false;
        }

        LOG(INFO) << "configuration " << cfg
            << "debug:" << debug_ << " "
            << "mode:" << mode_ << " "
            << "duration:" << duration_ << " "
            << "transactions:" << transactions_ << " "
            << "statements:" << statements_ << " "
            << "clients:" << clients_ << " "
            << "ltx:" << ltx_ << " "
            << "rtx:" << rtx_ << " "
            << "client_idle:" << client_idle_ << " "
            << "";

        return true;
    }

    struct data_seed {
        explicit data_seed(std::size_t i, std::int64_t seq) :
            rnd_(static_cast<std::uint32_t>(123456+i)),
            seq_(seq)
        {}
        jogasaki::utils::xorshift_random32 rnd_{};  //NOLINT
        std::int64_t seq_{};  //NOLINT
    };

    bool prepare_statement(jogasaki::common_cli::service_client& client, std::uint64_t& stmt) {
        std::string insert_clause("INSERT ");
        switch(mode_) {
            case mode::upsert:
                insert_clause = "INSERT OR REPLACE ";
                // fall-thru
            case mode::insert:
                return prepare_sql(client, stmt, insert_clause+"INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (:no_o_id, :no_d_id, :no_w_id)",
                    {
                        {"no_o_id", sql::common::AtomType::INT8},
                        {"no_d_id", sql::common::AtomType::INT8},
                        {"no_w_id", sql::common::AtomType::INT8},
                    }
                );
            case mode::update:
                return prepare_sql(client, stmt, "UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id",
                    {
                        {"s_quantity", sql::common::AtomType::INT8},
                        {"s_i_id", sql::common::AtomType::INT8},
                        {"s_w_id", sql::common::AtomType::INT8},
                    }
                );
            case mode::query:
                return prepare_sql(client, stmt, "SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id",
                    {
                        {"d_w_id", sql::common::AtomType::INT8},
                        {"d_id", sql::common::AtomType::INT8},
                    }
                );
            case mode::query2:
                return prepare_sql(client, stmt, "SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id",
                    {
                        {"no_d_id", sql::common::AtomType::INT8},
                        {"no_w_id", sql::common::AtomType::INT8},
                    }
                );
            default:
                std::abort();
        }
    }
    bool do_statement(
        jogasaki::common_cli::service_client& client,
        std::uint64_t stmt,
        jogasaki::api::transaction_handle tx_handle,
        data_seed& seed,
        std::size_t client_index,
        std::int64_t& result_count
    ) {
        bool res{};
        switch(mode_) {
            case mode::upsert: // fall-thru
            case mode::insert: {
                std::int64_t id = profile_.new_order_max_ + (seed.seq_++);
                res = issue_common(client, stmt, false,
                    tx_handle,
                    std::vector<jogasaki::utils::parameter>{
                        {"no_o_id", ValueCase::kInt8Value, id},
                        {"no_d_id", ValueCase::kInt8Value, static_cast<int64_t>(1)},
                        {"no_w_id", ValueCase::kInt8Value, static_cast<int64_t>(client_index+1)},
                    },
                    {}
                );
                break;
            }
            case mode::update: {
                auto mod = profile_.stock_item_id_max_ > profile_.stock_item_id_min_ ?
                    profile_.stock_item_id_max_ - profile_.stock_item_id_min_ : 1;
                std::int64_t id = profile_.stock_item_id_min_ + seed.rnd_() % mod;
                std::int64_t w_id = FLAGS_prepare_data ? id : (client_index+1);
                res = issue_common(client, stmt, false,
                    tx_handle,
                    std::vector<jogasaki::utils::parameter>{
                        {"s_quantity", ValueCase::kInt8Value, static_cast<std::int64_t>(seed.rnd_() % 100)}, //NOLINT
                        {"s_i_id", ValueCase::kInt8Value, id},
                        {"s_w_id", ValueCase::kInt8Value, w_id},
                    },
                    {}
                );
                break;
            }
            case mode::query: {
                auto mod = profile_.district_id_max_ > profile_.district_id_min_ ?
                    profile_.district_id_max_ - profile_.district_id_min_: 1;
                std::int64_t id = profile_.district_id_min_ + seed.rnd_() % mod;
                std::int64_t w_id = FLAGS_prepare_data ? id : (client_index+1);
                res = issue_common(client, stmt, true,
                    tx_handle,
                    std::vector<jogasaki::utils::parameter>{
                        {"d_w_id", ValueCase::kInt8Value, w_id},
                        {"d_id", ValueCase::kInt8Value, static_cast<std::int64_t>(id)},
                    },
                    [&](std::string_view data) {
                        DVLOG(jogasaki::log_debug) << "write: " << jogasaki::utils::binary_printer{data.data(), data.size()};
                        ++result_count;
                        if (verify_query_records_) {
                            std::unique_lock lk{write_buffer_mutex_};
                            write_buffer_.write(data.data(), data.size());
                            write_buffer_.flush();
                        }
                    });
                break;
            }
            case mode::query2: {
                std::int64_t w_id = FLAGS_prepare_data ? 1 : (client_index+1);
                res = issue_common(client, stmt, true,
                    tx_handle,
                    std::vector<jogasaki::utils::parameter>{
                        {"no_d_id", ValueCase::kInt8Value, static_cast<std::int64_t>(1)},
                        {"no_w_id", ValueCase::kInt8Value, w_id},
                    },
                    [&](std::string_view data) {
                        DVLOG(jogasaki::log_debug) << "write: " << jogasaki::utils::binary_printer{data.data(), data.size()};
                        ++result_count;
                        if (verify_query_records_) {
                            std::unique_lock lk{write_buffer_mutex_};
                            write_buffer_.write(data.data(), data.size());
                            write_buffer_.flush();
                        }
                    });
                break;
            }
            default:
                std::abort();
        }
        if(!res) {
            LOG(ERROR) << "query error";
            std::abort();
        }
        return true;
    }

    jogasaki::common_cli::service_client create_client(std::size_t session_id) {
        return {env_.service(), session_id, {jogasaki::common_cli::wait_kind::spin, 60000ms}};
    }

    bool run_workers() {
        std::vector<std::future<result_info>> results{};
        results.reserve(clients_);
        std::atomic_bool stop = false;
        boost::latch start{clients_};
        for(std::size_t i=0; i < clients_; ++i) {
            results.emplace_back(
                std::async(std::launch::async, [&, i](){
                    if (FLAGS_client_initial_core != -1) {
                        utils::set_thread_affinity(i,
                            {
                                utils::affinity_tag<utils::affinity_kind::core_affinity>,
                                static_cast<std::size_t>(FLAGS_client_initial_core)
                            }
                        );
                    } else {
                        // by default assign the on numa nodes uniformly
                        utils::set_thread_affinity(i, utils::affinity_profile{
                            utils::affinity_tag<utils::affinity_kind::numa_affinity>
                        });
                    }
                    // each client has its own session, so the statement is prepared on the session
                    auto client = create_client(i+1);
                    std::uint64_t stmt{};
                    if (! prepare_statement(client, stmt)) {
                        std::abort();
                    }
                    jogasaki::common_cli::begin_options options{};
                    options.kind_ = rtx_ ? jogasaki::common_cli::begin_options::kind::rtx :
                        (ltx_ ? jogasaki::common_cli::begin_options::kind::ltx : jogasaki::common_cli::begin_options::kind::occ);
                    if (ltx_) {
                        options.write_preserves_ = {"NEW_ORDER", "STOCK"};
                    }
                    result_info ret{};
                    start.count_down_and_wait();
                    data_seed seed{i, 0};
                    while((transactions_ == -1 && !stop) || (transactions_ != -1 && ret.transactions_ < transactions_)) {
                        jogasaki::api::transaction_handle tx_handle{};
                        {
                            auto b = clock ::now();
                            if (auto res = client.begin(options, tx_handle); !res) {
                                report_error("begin", res);
                                std::abort();
                            }
                            ret.begin_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - b).count();
                        }
                        for(std::size_t j=0, n=statements_; j < n; ++j) {
                            {
                                auto b = clock::now();
                                if(auto res = do_statement(client, stmt, tx_handle, seed, i, ret.records_); !res) {
                                    LOG(ERROR) << "do_statement failed";
                                    std::abort();
                                }
                                ret.statement_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - b).count();
                            }
                            ++ret.statements_;
                            if (transactions_ == -1 && stop) {
                                break;
                            }
                            if (client_idle_ > 0) {
                                auto cn = 0;
                                while(cn < client_idle_) {
                                    _mm_pause();
                                    ++cn;
                                }
                            }
                        }
                        ++ret.transactions_;
                        {
                            auto b = clock ::now();
                            if (auto res = client.commit(tx_handle); !res) {
                                report_error("commit", res);
                                std::abort();
                            }
                            ret.commit_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - b).count();
                        }
                    }
                    (void) client.dispose_statement(stmt);
                    return ret;
                })
            );
        }

        start.wait();
        auto begin = clock::now();
        if (transactions_ == -1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(duration_));
            stop = true;
        }
        result_info total_result{};
        for(auto&& f : results) {
            auto r = f.get();
            total_result.transactions_ += r.transactions_;
            total_result.statements_ += r.statements_;
            total_result.records_ += r.records_;
            total_result.begin_ns_ += r.begin_ns_;
            total_result.statement_ns_ += r.statement_ns_;
            total_result.commit_ns_ += r.commit_ns_;
        }
        results.clear();
        auto end = clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-begin).count();
        show_result(total_result, duration_ms, clients_, md_);
        return true;
    }

    void execute_statement(std::string_view stmt) {
        std::string msg = std::string{jogasaki::utils::runner{}
            .db(env_.database())
            .show_plan(false)
            .show_recs(false)
            .text(stmt)
            .run()
            .report()};
        if (!msg.empty()) {
            throw_exception(std::runtime_error{msg});
        }
    }
    void setup_tables() {
        execute_statement(
            "CREATE TABLE NEW_ORDER ("
            "no_o_id INT NOT NULL, "
            "no_d_id INT NOT NULL, "
            "no_w_id INT NOT NULL, "
            "PRIMARY KEY(no_w_id, no_d_id, no_o_id))");
        if(secondary_index_count_ > 0) {
            execute_statement("CREATE INDEX NEW_ORDER_IDX1 ON NEW_ORDER(no_w_id)");
        }
        execute_statement(
               "CREATE TABLE DISTRICT ("
               "d_id INT NOT NULL, "
               "d_w_id INT NOT NULL, "
               "d_name VARCHAR(10) NOT NULL, "
               "d_street_1 VARCHAR(20) NOT NULL, "
               "d_street_2 VARCHAR(20) NOT NULL, "
               "d_city VARCHAR(20) NOT NULL, "
               "d_state CHAR(2) NOT NULL, "
               "d_zip  CHAR(9) NOT NULL, "
               "d_tax DOUBLE NOT NULL, "
               "d_ytd DOUBLE NOT NULL, "
               "d_next_o_id INT NOT NULL, "
               "PRIMARY KEY(d_w_id, d_id))"
        );
        execute_statement(
            "CREATE TABLE STOCK ("
            "s_i_id INT NOT NULL, "
            "s_w_id INT NOT NULL, "
            "s_quantity INT NOT NULL, "
            "s_dist_01 CHAR(24) NOT NULL, "
            "s_dist_02 CHAR(24) NOT NULL, "
            "s_dist_03 CHAR(24) NOT NULL, "
            "s_dist_04 CHAR(24) NOT NULL, "
            "s_dist_05 CHAR(24) NOT NULL, "
            "s_dist_06 CHAR(24) NOT NULL, "
            "s_dist_07 CHAR(24) NOT NULL, "
            "s_dist_08 CHAR(24) NOT NULL, "
            "s_dist_09 CHAR(24) NOT NULL, "
            "s_dist_10 CHAR(24) NOT NULL, "
            "s_ytd INT NOT NULL, "
            "s_order_cnt INT NOT NULL, "
            "s_remote_cnt INT NOT NULL, "
            "s_data VARCHAR(50) NOT NULL, "
            "PRIMARY KEY(s_w_id, s_i_id))");
    }
    bool setup_db(std::shared_ptr<jogasaki::configuration> cfg) {
        auto begin = clock::now();
        if (! env_.start(std::move(cfg))) {
            return false;
        }
        if(ddl_) {
            setup_tables();
        } else if (! jogasaki::common_cli::setup_builtin_tables(env_.database())) {
            return false;
        }
        auto end = clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-begin).count();
        LOG(INFO) << "setup duration: " << format(duration_ms) << " ms";
        return true;
    }

    int run(std::shared_ptr<jogasaki::configuration> cfg) {
        if (! setup_db(std::move(cfg))) {
            return -1;
        }
        // validate the statement before starting clients
        auto client = create_client(clients_+1);
        std::uint64_t stmt{};
        if (! prepare_statement(client, stmt)) {
            env_.stop();
            return -1;
        }
        (void) client.dispose_statement(stmt);
        run_workers();
        env_.stop();
        return 0;
    }

private:
    static void report_error(std::string_view operation, jogasaki::common_cli::call_result const& res) {
        auto& diag = res.diag();
        LOG(ERROR) << operation << " failed " << sql::error::Code_Name(diag.code_) << ": " << diag.message_
            << (diag.supplemental_text_.empty() ? "" : " supplemental text: ") << diag.supplemental_text_;
    }

    bool prepare_sql(
        jogasaki::common_cli::service_client& client,
        std::uint64_t& stmt,
        std::string_view sql,
        std::unordered_map<std::string, sql::common::AtomType> const& place_holders
    ) {
        if (auto res = client.prepare(sql, place_holders, stmt); ! res) {
            report_error("prepare", res);
            return false;
        }
        VLOG(jogasaki::log_debug) << "statement prepared: session(" << client.session_id() << ") handle(" << stmt << ") " << sql;
        return true;
    }

    void reset_write_buffer() {
        write_buffer_.str("");
        write_buffer_.clear();
    }

    bool issue_common(
        jogasaki::common_cli::service_client& client,
        std::uint64_t stmt,
        bool query,
        jogasaki::api::transaction_handle tx_handle,
        std::vector<jogasaki::utils::parameter> const& parameters,
        jogasaki::common_cli::data_handler on_write
    ) {
        if (! query) {
            if (auto res = client.execute_prepared_statement(tx_handle, stmt, parameters); ! res) {
                report_error("execute statement", res);
                return false;
            }
            return true;
        }
        reset_write_buffer();
        jogasaki::common_cli::query_info info{};
        if (auto res = client.execute_prepared_query(
                tx_handle,
                stmt,
                parameters,
                std::move(on_write),
                verify_query_records_ ? &info : nullptr
            ); ! res) {
            report_error("execute query", res);
            return false;
        }
        if (verify_query_records_) {
            DVLOG(jogasaki::log_debug) << "query name : " << info.name_;
            query_meta_ = jogasaki::utils::create_record_meta(info.columns_);
            std::size_t ind{};
            for(auto&& f : query_meta_) {
                DVLOG(jogasaki::log_debug) << "column " << ind << ": " << f;
                ++ind;
            }
            auto recs = jogasaki::utils::deserialize_msg(
                tateyama::api::server::mock::view_of(write_buffer_),
                query_meta_
            );
            for(auto&& r : recs) {
                DVLOG(jogasaki::log_debug) << "record : " << r;
            }
        }
        return true;
    }
};

}  // namespace

tateyama::service_benchmark::cli* cli_ = nullptr;

void sighup_handler(int) {
    cli_->print_diagnostics();
}

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("service benchmark");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("service benchmark");
    // defaults of this benchmark, kept from the ones before using common flags
    jogasaki::common_cli::set_flag_defaults({
        {"thread_count", "1"},
        {"assign_numa_nodes_uniformly", "true"},
        {"busy_worker", "true"},
    });
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    tateyama::service_benchmark::cli c{};
    cli_ = std::addressof(c);
    if (signal(SIGHUP, sighup_handler) == SIG_ERR) {
        LOG(ERROR) << "failed to set sighup signal handler";
    }
    auto cfg = std::make_shared<jogasaki::configuration>();
    if (! c.fill_from_flags(*cfg)) {
        return -1;
    }
    try {
        return c.run(cfg);  // NOLINT
    } catch (std::exception& e) {
        LOG(ERROR) << e.what();
        return -1;
    }
}
