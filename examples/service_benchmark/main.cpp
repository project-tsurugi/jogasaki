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
#include <memory>
#include <future>
#include <chrono>

#include <glog/logging.h>
#include <boost/thread/latch.hpp>

#include <takatori/util/fail.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/utils/thread_affinity.h>

#include <jogasaki/api.h>
#include <jogasaki/logging.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/utils/latch.h>
#include <jogasaki/utils/storage_data.h>

#include "../common/load.h"
#include "../common/temporary_folder.h"
#include "../common/utils.h"
#include "../query_bench_cli/utils.h"

DEFINE_bool(single_thread, false, "Whether to run on serial scheduler");  //NOLINT
DEFINE_int32(thread_count, 1, "Number of threads used in server thread pool");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_int32(partitions, 10, "Number of partitions per process");  //NOLINT
DEFINE_bool(steal, false, "Enable stealing for task scheduling");  //NOLINT
DEFINE_int32(prepare_data, 0, "Whether to prepare records in the storages. Specify 0 to disable.");  //NOLINT
DEFINE_bool(verify, false, "Whether to deserialize the query result records. Requires clients=1");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_string(location, "TMP", "specify the database directory. Pass TMP to use temporary directory.");  //NOLINT
DEFINE_string(load_from, "", "specify the generated db file directory. Use to prepare initial data.");  //NOLINT
DECLARE_int32(dump_batch_size);  //NOLINT
DECLARE_int32(load_batch_size);  //NOLINT
DEFINE_bool(insert, false, "run on insert mode");  //NOLINT
DEFINE_bool(update, false, "run on update mode");  //NOLINT
DEFINE_bool(query, false, "run on query mode (point query)");  //NOLINT
DEFINE_bool(query2, false, "run on query mode with multiple records");  //NOLINT
DEFINE_int32(statements, 1000, "The number of statements issued per transaction.");  //NOLINT
DEFINE_int64(duration, 5000, "Run duration in milli-seconds");  //NOLINT
DEFINE_int64(transactions, -1, "Number of transactions executed per client thread. Specify -1 to use duration instead.");  //NOLINT
DEFINE_int32(clients, 1, "Number of client threads");  //NOLINT
DEFINE_int32(client_initial_core, -1, "set the client thread core affinity and assign sequentially from the specified core. Specify -1 not to set core-level thread affinity, then threads are distributed on numa nodes uniformly.");  //NOLINT
DEFINE_bool(tasked_insert, true, "run insert as task");  //NOLINT

namespace tateyama::service_benchmark {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;
using namespace jogasaki::query_bench_cli;
using tateyama::api::server::response_code;

using takatori::util::unsafe_downcast;

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
    undefined, insert, update, query, query2
};

[[nodiscard]] constexpr inline std::string_view to_string_view(mode value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case mode::undefined: return "undefined"sv;
        case mode::insert: return "insert"sv;
        case mode::update: return "update"sv;
        case mode::query: return "query"sv;
        case mode::query2: return "query2"sv;
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, mode value) {
    return out << to_string_view(value);
}

void show_result(
    result_info result,
    std::size_t duration_ms,
    std::size_t threads
) {
    auto& transactions = result.transactions_;
    auto& statements = result.statements_;
    auto& records = result.records_;
    auto transaction_ns = result.commit_ns_ + result.statement_ns_ + result.commit_ns_;
    auto& statement_ns = result.statement_ns_;

    LOG(INFO) << "duration: " << format(duration_ms) << " ms";
    LOG(INFO) << "  transactions took : " << format((std::int64_t)(double)transaction_ns / threads) << " ns/thread";
    LOG(INFO) << "  statements took : " << format((std::int64_t)(double)statement_ns / threads) << " ns/thread";
    LOG(INFO) << "executed: " <<
        format(transactions) << " transactions, " <<
        format(statements) << " statements, " <<
        format(records) << " records";
    LOG(INFO) << "throughput: " <<
        format((std::int64_t)((double)transactions / duration_ms * 1000)) << " transactions/s, " <<  //NOLINT
        format((std::int64_t)((double)statements / statement_ns * threads * 1000 * 1000 * 1000)) << " statements/s, " <<  //NOLINT
        format((std::int64_t)((double)records / statement_ns * threads * 1000 * 1000 * 1000)) << " records/s";  //NOLINT
    LOG(INFO) << "throughput/thread: " <<
        format((std::int64_t)((double)transactions / threads / duration_ms * 1000)) << " transactions/s/thread, " <<  //NOLINT
        format((std::int64_t)((double)statements / statement_ns * 1000 * 1000 * 1000)) << " statements/s/thread, " << //NOLINT
        format((std::int64_t)((double)records/ statement_ns * 1000 * 1000 * 1000)) << " records/s/thread";  //NOLINT
    LOG(INFO) << "avg turn-around: " <<
        "transaction " << format((std::int64_t)((double)duration_ms * 1000 * 1000 * threads / transactions)) << " ns, " <<  //NOLINT
        "statement " << format((std::int64_t)((double)statement_ns / statements)) << " ns, " <<  //NOLINT
        "record " << format((std::int64_t)((double)statement_ns / records)) << " ns";  //NOLINT
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
    takatori::util::maybe_shared_ptr<jogasaki::api::database> db_{};
    std::shared_ptr<jogasaki::api::impl::service> service_{};  //NOLINT
    bool debug_{}; //NOLINT
    bool verify_query_records_{};
    std::mutex write_buffer_mutex_{};
    std::stringstream write_buffer_{};
    std::uint64_t stmt_handle_{};
    std::vector<std::future<bool>> on_going_statements_{};
    jogasaki::meta::record_meta query_meta_{};
    jogasaki::common_cli::temporary_folder temporary_{};
    std::map<std::string, sql::common::AtomType> host_variables_{};

    mode mode_{mode::undefined};
    data_profile profile_{profile_v<profile::normal>};
    std::int64_t transactions_{};
    std::int64_t duration_{};
    std::int64_t statements_{};
    std::size_t clients_{};

public:

    void prepare_data(jogasaki::api::database& db, std::size_t rows) {
        auto& db_impl = unsafe_downcast<jogasaki::api::impl::database&>(db);
        static constexpr std::size_t mod = 100;
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "T0", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "T1", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "T2", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "WAREHOUSE", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "DISTRICT", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "CUSTOMER", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "NEW_ORDER", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "ORDERS", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "ORDER_LINE", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "ITEM", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "STOCK", rows, true, mod);
        jogasaki::utils::populate_storage_data(db_impl.kvs_db().get(), db_impl.tables(), "HISTORY", rows, true, mod);
    }

    bool fill_from_flags(
        jogasaki::configuration& cfg,
        std::string const& str = {}
    ) {
        gflags::FlagSaver saver{};
        if (! str.empty()) {
            if(! gflags::ReadFlagsFromString(str, "", false)) {
                LOG(ERROR) << "parsing options failed";
            }
        }
        cfg.single_thread(FLAGS_single_thread);
        cfg.thread_pool_size(FLAGS_thread_count);
        cfg.tasked_write(FLAGS_tasked_insert);

        cfg.core_affinity(FLAGS_core_affinity);
        cfg.initial_core(FLAGS_initial_core);
        cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
        cfg.default_partitions(FLAGS_partitions);
        cfg.stealing_enabled(FLAGS_steal);

        if (FLAGS_minimum) {
            cfg.single_thread(false);
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

        debug_ = FLAGS_debug;
        verify_query_records_ = FLAGS_verify;
        transactions_ = FLAGS_transactions;
        duration_ = FLAGS_duration;
        statements_ = FLAGS_statements;
        clients_ = FLAGS_clients;

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
        if (FLAGS_minimum) {
            mode_ = mode::insert;
            duration_ = -1;
            transactions_ = 1;
            statements_ = 1;
            clients_ = 1;
        }
        if (mode_ == mode::undefined) {
            LOG(ERROR) << "Specify one of --insert/--update/--query options.";
            return false;
        }

        LOG(INFO) << "configuration " << cfg
            << "debug:" << debug_ << " "
            << "mode:" << mode_ << " "
            << "duration:" << duration_ << " "
            << "transactions:" << transactions_ << " "
            << "statements:" << statements_ << " "
            << "clients:" << clients_ << " "
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

    void prepare_statement() {
        bool res;
        switch(mode_) {
            case mode::insert:
                res = prepare_sql("INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (:no_o_id, :no_d_id, :no_w_id)",
                    {
                        {"no_o_id", sql::common::AtomType::INT8},
                        {"no_d_id", sql::common::AtomType::INT8},
                        {"no_w_id", sql::common::AtomType::INT8},
                    }
                );
                break;
            case mode::update:
                res = prepare_sql("UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id",
                    {
                        {"s_quantity", sql::common::AtomType::FLOAT8},
                        {"s_i_id", sql::common::AtomType::INT8},
                        {"s_w_id", sql::common::AtomType::INT8},
                    }
                );
                break;
            case mode::query:
                res = prepare_sql("SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id",
                    {
                        {"d_w_id", sql::common::AtomType::INT8},
                        {"d_id", sql::common::AtomType::INT8},
                    }
                );
                break;
            case mode::query2:
                res = prepare_sql("SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id",
                    {
                        {"no_d_id", sql::common::AtomType::INT8},
                        {"no_w_id", sql::common::AtomType::INT8},
                    }
                );
                break;
            default:
                std::abort();
        }
        if (!res) {
            std::abort();
        }
    }
    bool do_statement(
        std::uint64_t handle,
        data_seed& seed,
        std::size_t client,
        std::int64_t& result_count
    ) {
        bool res{};
        switch(mode_) {
            case mode::insert: {
                result_count = 0;
                std::int64_t id = profile_.new_order_max_ + (seed.seq_++);
                res = issue_common(false,
                    handle,
                    std::vector<jogasaki::utils::parameter>{
                        {"no_o_id", ValueCase::kInt8Value, id},
                        {"no_d_id", ValueCase::kInt8Value, static_cast<int64_t>(1)},
                        {"no_w_id", ValueCase::kInt8Value, static_cast<int64_t>(client+1)},
                    },
                    {}
                );
                break;
            }
            case mode::update: {
                result_count = 0;
                auto mod = profile_.stock_item_id_max_ > profile_.stock_item_id_min_ ?
                    profile_.stock_item_id_max_ - profile_.stock_item_id_min_ : 1;
                std::int64_t id = profile_.stock_item_id_min_ + seed.rnd_() % mod;
                res = issue_common(false,
                    handle,
                    std::vector<jogasaki::utils::parameter>{
                        {"s_quantity", ValueCase::kFloat8Value, static_cast<double>(seed.rnd_())}, //NOLINT
                        {"s_i_id", ValueCase::kInt8Value, id},
                        {"s_w_id", ValueCase::kInt8Value, static_cast<std::int64_t>(client+1)},
                    },
                    {}
                );
                break;
            }
            case mode::query: {
                auto mod = profile_.district_id_max_ > profile_.district_id_min_ ?
                    profile_.district_id_max_ - profile_.district_id_min_: 1;
                std::int64_t id = profile_.district_id_min_ + seed.rnd_() % mod;
                res = issue_common(true,
                    handle,
                    std::vector<jogasaki::utils::parameter>{
                        {"d_w_id", ValueCase::kInt8Value, static_cast<std::int64_t>(client+1)},
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
                res = issue_common(true,
                    handle,
                    std::vector<jogasaki::utils::parameter>{
                        {"no_d_id", ValueCase::kInt8Value, static_cast<std::int64_t>(1)},
                        {"no_w_id", ValueCase::kInt8Value, static_cast<std::int64_t>(client+1)},
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

    bool run_workers(std::shared_ptr<jogasaki::configuration> cfg) {
        (void)cfg;
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
                    result_info ret{};
                    start.count_down_and_wait();
                    data_seed seed{i, 0};
                    std::uint64_t handle{};
                    {
                        auto b = clock ::now();
                        if (auto res = begin_tx(handle); !res) {
                            std::abort();
                        }
                        ret.begin_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - b).count();
                    }
                    while((transactions_ == -1 && !stop) || (transactions_ != -1 && ret.transactions_ < transactions_)) {
                        for(std::size_t j=0, n=statements_; j < n; ++j) {
                            {
                                auto b = clock::now();
                                if(auto res = do_statement(handle, seed, i, ret.records_); !res) {
                                    LOG(ERROR) << "do_statement failed";
                                }
                                ret.statement_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - b).count();
                            }
                            ++ret.statements_;
                            if (transactions_ == -1 && stop) {
                                break;
                            }
                        }
                        ++ret.transactions_;
                    }
                    {
                        auto b = clock ::now();
                        if (auto res = commit_tx(handle); !res) {
                            LOG(ERROR) << "commit_tx failed";
                        }
                        ret.commit_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - b).count();
                    }
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
        auto duration_ms = std::chrono::duration_cast<clock::duration>(end-begin).count()/1000/1000;
        show_result(total_result, duration_ms, clients_);
        return true;
    }

    void setup_db(
        std::shared_ptr<jogasaki::configuration> cfg,
        jogasaki::common_cli::temporary_folder& dir
    ) {
        if (FLAGS_location == "TMP") {
            dir.prepare();
            cfg->db_location(dir.path());
        } else {
            cfg->db_location(std::string(FLAGS_location));
        }

        db_ = std::shared_ptr{jogasaki::api::create_database(cfg)};
        auto c = std::make_shared<tateyama::api::configuration::whole>("");
        service_ = std::make_shared<jogasaki::api::impl::service>(c, db_.get());
        db_->start();

        auto& impl = jogasaki::api::impl::get_impl(*db_);
        jogasaki::executor::register_kvs_storage(*impl.kvs_db(), *impl.tables());
        if (! FLAGS_load_from.empty()) {
            jogasaki::common_cli::load(*db_, FLAGS_load_from);
        }
        if (FLAGS_prepare_data > 0) {
            prepare_data(*db_, FLAGS_prepare_data);
        }
    }

    int run(std::shared_ptr<jogasaki::configuration> cfg) {
        jogasaki::common_cli::temporary_folder dir{};
        setup_db(cfg, dir);
        prepare_statement();
        run_workers(cfg);
        db_->stop();
        dir.clean();
        return 0;
    }

private:
    bool begin_tx(std::uint64_t& handle) {
        auto s = jogasaki::utils::encode_begin(false);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        while(! res->completed()) { /* noop */ }
        if(! st || !res->completed() || res->code_ != response_code::success) {
            LOG(ERROR) << "error executing command";
            return false;
        }
        auto [h, id] = jogasaki::utils::decode_begin(res->body_);
        handle = h;
        (void) id;
        return true;
    }

    bool handle_result_only(std::string_view body) {
        auto [success, error] = jogasaki::utils::decode_result_only(body);
        if (success) {
            return true;
        }
        LOG(ERROR) << "command returned " << sql::status::Status_Name(error.status_) << ": " << error.message_;
        return false;
    }

    // block wait for async query execution
    void wait_for_statements() {
        for(auto&& f : on_going_statements_) {
            (void)f.get();
        }
        on_going_statements_.clear();
    }

    bool commit_tx(std::uint64_t handle) {
        auto s = jogasaki::utils::encode_commit(handle);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        if(! st || !res->completed() || res->code_ != response_code::success) {
            LOG(ERROR) << "error executing command";
        }
        auto ret = handle_result_only(res->body_);
        wait_for_statements(); // just for cleanup
        return ret;
    }

    bool abort_tx(std::uint64_t handle) {
        wait_for_statements();
        auto s = jogasaki::utils::encode_rollback(handle);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        if(! st || !res->completed() || res->code_ != response_code::success) {
            LOG(ERROR) << "error executing command";
        }
        auto ret = handle_result_only(res->body_);
        wait_for_statements(); // just for cleanup
        return ret;
    }

    bool prepare_sql(
        std::string_view sql,
        std::unordered_map<std::string, sql::common::AtomType> const& place_holders
    ) {
        auto s = jogasaki::utils::encode_prepare_vars(std::string{sql}, place_holders);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        if(! st || !res->completed() || res->code_ != response_code::success) {
            LOG(ERROR) << "error executing command";
            return false;
        }
        stmt_handle_ = jogasaki::utils::decode_prepare(res->body_);

        LOG(INFO) << "statement prepared: handle(" << stmt_handle_ << ") " << sql;
        return true;
    }

    void reset_write_buffer() {
        write_buffer_.str("");
        write_buffer_.clear();
    }

    bool issue_common(
        bool query,
        std::uint64_t handle,
        std::vector<jogasaki::utils::parameter> const& parameters,
        std::function<void(std::string_view)> on_write
    ) {
        auto s = query ?
            jogasaki::utils::encode_execute_prepared_query(handle, stmt_handle_, parameters) :
            jogasaki::utils::encode_execute_prepared_statement(handle, stmt_handle_, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        if (query) {
            res->set_on_write(std::move(on_write));
        }
        reset_write_buffer();
        auto st = (*service_)(req, res);
        if(! st) {
            LOG(ERROR) << "service invocation failed";
            return false;
        }
        if (query && verify_query_records_) {
            auto [name, columns] = jogasaki::utils::decode_execute_query(res->body_head_);
            DVLOG(jogasaki::log_debug) << "query name : " << name;
            query_meta_ = jogasaki::utils::create_record_meta(columns);
            std::size_t ind{};
            for(auto&& f : query_meta_) {
                DVLOG(jogasaki::log_debug) << "column " << ind << ": " << f;
                ++ind;
            }
        }

        // making lambda in order to change this to async
        auto wait_completion = [&, res]() {
            if(! res->wait_completion(60000ms)) {
                LOG(ERROR) << "execution took too long";
                std::abort();
            }
            if(res->code_ != response_code::success) {
                LOG(ERROR) << "error executing command";
            }
            if (verify_query_records_) {
                auto recs = jogasaki::utils::deserialize_msg(
                    tateyama::api::server::mock::view_of(write_buffer_),
                    query_meta_
                );
                for(auto&& r : recs) {
                    DVLOG(jogasaki::log_debug) << "record : " << r;
                }
            }
            return handle_result_only(res->body_);
        };
        return wait_completion();
    }
};

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("service benchmark");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("service benchmark");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    tateyama::service_benchmark::cli e{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    cfg->prepare_benchmark_tables(true);
    if (! e.fill_from_flags(*cfg)) {
        return -1;
    }
    try {
        e.run(cfg);  // NOLINT
    } catch (std::exception& e) {
        LOG(ERROR) << e.what();
        return -1;
    }
    return 0;
}
