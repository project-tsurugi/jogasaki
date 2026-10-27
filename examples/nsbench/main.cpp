/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <jogasaki/configuration.h>
#include <jogasaki/proto/sql/error.pb.h>

#include "entity.h"
#include "report.h"
#include "service_client.h"
#include "service_setup.h"

// options compatible with simple-benchmark (sbench). Option names are same as sbench.
DEFINE_string(connection, "ipc:tsurugi", "ignored - for compatibility with sbench. The database runs in-process.");  //NOLINT
DEFINE_int32(session_timeout, 5, "ignored - for compatibility with sbench");  //NOLINT
DEFINE_string(session_shutdown, "nothing", "ignored - for compatibility with sbench");  //NOLINT
DEFINE_bool(auto_inc, false, "ignored - for compatibility with sbench (not supported by sbench either)");  //NOLINT
DEFINE_string(output, "", "csv file name to append the result (-o). With -st/-mt commands and .csv extension, results of each thread are written to _t<N>.csv. Histogram is written to _hist.csv.");  //NOLINT
DEFINE_bool(header, false, "write the csv header to the output file (-h)");  //NOLINT
DEFINE_int64(duration, 0, "duration in seconds (-d). Benchmark finishes after the duration even if operations remain. 0 to run until operations are done.");  //NOLINT
DEFINE_int64(interval, 0, "interval in milliseconds between transactions (-i). Effective for insert only (same as sbench).");  //NOLINT
DEFINE_string(transaction, "OCC", "transaction type (-t): OCC/short, LTX/long, or RO/read/readonly");  //NOLINT
DEFINE_string(entity, "lkv", "entity kind (-e): lkv or sysbench");  //NOLINT
DEFINE_string(read_area, "none", "read area for LTX: none or inclusive");  //NOLINT
DEFINE_int32(table_id, 1, "id of the table for single thread commands");  //NOLINT
DEFINE_int64(operations, 0, "number of operations. 0 means 100 if --duration=0, otherwise 1000000000");  //NOLINT
DEFINE_int64(operations_per_tx, -1, "number of operations per transaction. 0 to run all operations in a transaction. Defaults to 1 for insert/update, 0 for select/delete.");  //NOLINT
DEFINE_int64(id_origin, 1, "initial value of id for single thread commands");  //NOLINT
DEFINE_string(insert_mode, "INSERT", "INSERT or UPSERT (insert or replace)");  //NOLINT
DEFINE_int32(threads, 1, "number of threads for -st commands");  //NOLINT
DEFINE_int32(tables, 1, "number of tables for prepare and -mt commands");  //NOLINT
DEFINE_int64(max_value, 0, "select: maximum value specified for the condition. 0 means --operations");  //NOLINT
DEFINE_int64(scan_range, -1, "select: 0 full scan, 1 point read (default), >=2 range scan. delete: 0 all delete (default), 1 point delete, >=2 scan delete");  //NOLINT
DEFINE_string(select_columns, "*", "select columns ('*' or csv format: id,k,c,pad). Commas are replaced with spaces as sbench does.");  //NOLINT
DEFINE_string(condition_column, "", "column used in the select condition. Defaults to the primary key column.");  //NOLINT
DEFINE_string(index, "", "prepare: columns to create secondary index (csv format: k,c,pad)");  //NOLINT
DEFINE_bool(without_pk, false, "prepare: create tables without primary key (--without-pk)");  //NOLINT
DEFINE_int64(id_coefficient, 10000000, "id range assigned to each thread of -st commands (idCoefficient system property of sbench)");  //NOLINT

// options specific to nsbench
DEFINE_bool(client_spin, true, "client threads wait for the responses with busy loop. Specify false to sleep while waiting.");  //NOLINT

namespace tateyama::nsbench {

using clock = std::chrono::steady_clock;
namespace sql = jogasaki::proto::sql;
using jogasaki::common_cli::begin_options;
using jogasaki::common_cli::call_result;
using jogasaki::common_cli::service_client;

enum class base_command {
    prepare,
    insert,
    update,
    select,
    remove,  // delete
    noptx,
};

enum class variant {
    single,
    single_table,  // -st: multiple threads on a table
    multiple_table,  // -mt: a thread per table
};

struct command_spec {
    std::string name_{};
    base_command base_{};
    variant variant_{};
};

// supported commands. Commands of sbench not listed here (kvs-*, connect*, upsert-samekey*, stub-*) are not supported.
std::map<std::string, command_spec, std::less<>> const& commands() {
    static std::map<std::string, command_spec, std::less<>> ret{
        {"prepare", {"prepare", base_command::prepare, variant::single}},
        {"insert", {"insert", base_command::insert, variant::single}},
        {"insert-st", {"insert-st", base_command::insert, variant::single_table}},
        {"insert-mt", {"insert-mt", base_command::insert, variant::multiple_table}},
        {"update", {"update", base_command::update, variant::single}},
        {"update-st", {"update-st", base_command::update, variant::single_table}},
        {"update-mt", {"update-mt", base_command::update, variant::multiple_table}},
        {"select", {"select", base_command::select, variant::single}},
        {"select-st", {"select-st", base_command::select, variant::single_table}},
        {"select-mt", {"select-mt", base_command::select, variant::multiple_table}},
        {"delete", {"delete", base_command::remove, variant::single}},
        {"delete-mt", {"delete-mt", base_command::remove, variant::multiple_table}},
        {"noptx", {"noptx", base_command::noptx, variant::single}},
        {"noptx-st", {"noptx-st", base_command::noptx, variant::single_table}},
    };
    return ret;
}

std::string_view base_name(base_command b) {
    switch(b) {
        case base_command::prepare: return "prepare";
        case base_command::insert: return "insert";
        case base_command::update: return "update";
        case base_command::select: return "select";
        case base_command::remove: return "delete";
        case base_command::noptx: return "noptx";
    }
    std::abort();
}

std::string to_upper(std::string_view s) {
    std::string ret{s};
    std::transform(ret.begin(), ret.end(), ret.begin(), [](unsigned char c) { return std::toupper(c); });
    return ret;
}

/**
 * @brief arguments for a thread running a benchmark scenario (corresponds to the sbench argument of a scenario)
 */
struct scenario_arg {
    base_command base_{};
    std::int64_t table_id_{1};
    std::int64_t id_origin_{1};
    std::int64_t operations_{};
    std::int64_t operations_per_tx_{};
    std::int64_t max_value_{};
    std::int64_t scan_range_{};
    std::string output_path_{};
};

struct scenario_result {
    std::vector<std::int64_t> tx_{};
    std::vector<std::int64_t> bc_{};
    std::int64_t elapsed_ms_{};
    std::int64_t operations_per_tx_{};
    bool success_{true};
};

class nsbench {
public:
    bool init(jogasaki::configuration& cfg) {
        if (! jogasaki::common_cli::fill_configuration(cfg)) {
            return false;
        }
        if (FLAGS_entity == "lkv") {
            entity_ = entity{entity::kind::lkv};
        } else if (FLAGS_entity == "sysbench") {
            entity_ = entity{entity::kind::sysbench};
        } else {
            LOG(ERROR) << "unsupported entity: " << FLAGS_entity << " (lkv or sysbench is available)";
            return false;
        }
        // keep the spelling of the value as sbench prints the transaction enum
        static std::map<std::string, std::pair<begin_options::kind, std::string>, std::less<>> const transactions{
            {"SHORT", {begin_options::kind::occ, "short"}},
            {"OCC", {begin_options::kind::occ, "OCC"}},
            {"LONG", {begin_options::kind::ltx, "long"}},
            {"LTX", {begin_options::kind::ltx, "LTX"}},
            {"READ", {begin_options::kind::rtx, "read"}},
            {"READONLY", {begin_options::kind::rtx, "readonly"}},
            {"RO", {begin_options::kind::rtx, "RO"}},
        };
        if (auto it = transactions.find(to_upper(FLAGS_transaction)); it != transactions.end()) {
            tx_kind_ = it->second.first;
            tx_name_ = it->second.second;
        } else {
            LOG(ERROR) << "unsupported transaction: " << FLAGS_transaction;
            return false;
        }
        auto read_area = to_upper(FLAGS_read_area);
        if (read_area == "INCLUSIVE") {
            inclusive_read_area_ = true;
        } else if (read_area != "NONE") {
            LOG(ERROR) << "unsupported read area: " << FLAGS_read_area << " (none or inclusive is available)";
            return false;
        }
        auto insert_mode = to_upper(FLAGS_insert_mode);
        if (insert_mode != "INSERT" && insert_mode != "UPSERT") {
            LOG(ERROR) << "unsupported insert mode: " << FLAGS_insert_mode;
            return false;
        }
        upsert_ = insert_mode == "UPSERT";
        return true;
    }

    int run(std::shared_ptr<jogasaki::configuration> cfg, std::vector<command_spec> const& specs) {
        if (! env_.start(std::move(cfg))) {
            return -1;
        }
        bool success = true;
        for(auto&& spec : specs) {
            if (! run_command(spec)) {
                success = false;
                break;
            }
        }
        env_.stop();
        return success ? 0 : -1;
    }

private:
    jogasaki::common_cli::service_environment env_{};
    entity entity_{};
    begin_options::kind tx_kind_{};
    std::string tx_name_{};
    bool inclusive_read_area_{};
    bool upsert_{};
    std::atomic_size_t next_session_id_{1};
    std::atomic_bool stop_{};
    reporter reporter_{FLAGS_header};

    static void report_error(std::string_view operation, call_result const& res) {
        auto& diag = res.diag();
        LOG(ERROR) << operation << " failed " << sql::error::Code_Name(diag.code_) << ": " << diag.message_
            << (diag.supplemental_text_.empty() ? "" : " supplemental text: ") << diag.supplemental_text_;
    }

    service_client create_client() {
        auto kind = FLAGS_client_spin ? jogasaki::common_cli::wait_kind::spin : jogasaki::common_cli::wait_kind::sleep;
        return {env_.service(), next_session_id_++, {kind, std::chrono::milliseconds{0}}};
    }

    [[nodiscard]] begin_options tx_options(std::int64_t table_id) const {
        begin_options ret{};
        ret.kind_ = tx_kind_;
        if (tx_kind_ == begin_options::kind::ltx) {
            auto t = entity_.table_name(table_id);
            ret.write_preserves_.emplace_back(t);
            if (inclusive_read_area_) {
                ret.inclusive_read_areas_.emplace_back(t);
            }
        }
        return ret;
    }

    bool run_command(command_spec const& spec) {
        if (spec.base_ == base_command::prepare) {
            return prepare();
        }
        // default values that differ by command
        auto operations_per_tx = FLAGS_operations_per_tx;
        if (operations_per_tx < 0) {
            operations_per_tx = (spec.base_ == base_command::insert || spec.base_ == base_command::update) ? 1 : 0;
        }
        auto scan_range = FLAGS_scan_range;
        if (scan_range < 0) {
            scan_range = spec.base_ == base_command::select ? 1 : 0;
        }
        if (spec.base_ == base_command::select && operations_per_tx >= 1 && operations_per_tx < scan_range) {
            LOG(ERROR) << "--scan_range must be less than or equal to --operations_per_tx";
            return false;
        }
        std::vector<scenario_arg> args{};
        auto count = spec.variant_ == variant::single ? 1 : (spec.variant_ == variant::single_table ? FLAGS_threads : FLAGS_tables);
        for(std::int64_t i = 1; i <= count; ++i) {
            scenario_arg a{};
            a.base_ = spec.base_;
            a.operations_ = FLAGS_operations;
            a.operations_per_tx_ = operations_per_tx;
            a.max_value_ = FLAGS_max_value <= 0 ? FLAGS_operations : FLAGS_max_value;
            a.scan_range_ = scan_range;
            switch(spec.variant_) {
                case variant::single:
                    a.table_id_ = FLAGS_table_id;
                    a.id_origin_ = FLAGS_id_origin;
                    a.output_path_ = FLAGS_output;
                    break;
                case variant::single_table:
                    a.table_id_ = 1;
                    a.id_origin_ = i * FLAGS_id_coefficient;
                    break;
                case variant::multiple_table:
                    a.table_id_ = i;
                    a.id_origin_ = 1;
                    break;
            }
            if (spec.base_ == base_command::remove && spec.variant_ == variant::multiple_table) {
                // delete-mt only supports all-delete
                a.scan_range_ = 0;
                a.operations_ = 0;
                a.operations_per_tx_ = 0;
            }
            if (spec.variant_ != variant::single && FLAGS_output.size() >= 4 &&
                FLAGS_output.compare(FLAGS_output.size() - 4, 4, ".csv") == 0) {
                a.output_path_ = replace_all(FLAGS_output, ".csv", "_t" + std::to_string(i) + ".csv");
            }
            args.emplace_back(std::move(a));
        }
        if (spec.variant_ == variant::single) {
            std::cout << spec.name_ << " " << args[0].operations_ << " operations on '" << entity_.table_name(args[0].table_id_) << "' start" << std::endl;
        } else {
            std::cout << spec.name_ << " " << FLAGS_operations << " operations with '" << count
                << (spec.variant_ == variant::single_table ? " threads" : " tables") << "' start" << std::endl;
        }
        std::vector<scenario_result> results(args.size());
        stop_ = false;
        {
            std::vector<std::thread> threads{};
            threads.reserve(args.size());
            for(std::size_t i = 0; i < args.size(); ++i) {
                threads.emplace_back([&, i]() {
                    run_scenario(args[i], results[i]);
                });
            }
            for(auto&& t : threads) {
                t.join();
            }
        }
        for(auto&& r : results) {
            if (! r.success_) {
                return false;
            }
        }
        report(spec, args, results);
        return true;
    }

    std::string sub_command_name(base_command base, std::int64_t scan_range) const {
        switch(base) {
            case base_command::insert: return upsert_ ? "UPSERT" : "INSERT";
            case base_command::select:
                if (scan_range <= 0) return "full-scan";
                if (scan_range == 1) return "point-read";
                return "range-scan(" + std::to_string(scan_range) + ")";
            case base_command::remove:
                if (scan_range <= 0) return "all-delete";
                if (scan_range == 1) return "point-delete";
                return "scan-delete(" + std::to_string(scan_range) + ")";
            default: return "";
        }
    }

    result make_result(
        std::string command,
        scenario_arg const& arg,
        std::vector<stats> const& tx,
        std::vector<stats> const& bc,
        std::int64_t elapsed_ms,
        std::int64_t operations_per_tx,
        std::optional<std::int64_t> threads
    ) const {
        result ret{};
        ret.command_ = std::move(command);
        ret.sub_command_ = sub_command_name(arg.base_, arg.scan_range_);
        ret.threads_ = threads;
        ret.elapsed_ms_ = elapsed_ms;
        ret.operations_per_tx_ = operations_per_tx;
        ret.tx_ = tx.size() == 1 ? tx[0] : get_stats_of_stats(tx);
        if (arg.base_ != base_command::noptx) {
            ret.bc_ = bc.size() == 1 ? bc[0] : get_stats_of_stats(bc);
        }
        ret.transaction_ = tx_name_;
        return ret;
    }

    void report(command_spec const& spec, std::vector<scenario_arg> const& args, std::vector<scenario_result>& results) {
        std::vector<stats> tx{};
        std::vector<stats> bc{};
        std::vector<histogram> histograms{};
        double sum_elapsed{};
        for(std::size_t i = 0; i < results.size(); ++i) {
            auto& r = results[i];
            histogram h{};
            for(auto l : r.tx_) {
                h.add(l);
            }
            tx.emplace_back(get_stats(r.tx_));
            bc.emplace_back(get_stats(r.bc_));
            histograms.emplace_back(h);
            sum_elapsed += static_cast<double>(r.elapsed_ms_);
        }
        if (spec.variant_ == variant::single) {
            auto res = make_result(spec.name_, args[0], tx, bc, results[0].elapsed_ms_, results[0].operations_per_tx_, std::nullopt);
            res.histogram_ = histograms[0];
            reporter_.report(res, args[0].output_path_, false);
            return;
        }
        // report of each thread uses the base command name as sbench does
        histogram total{};
        for(std::size_t i = 0; i < results.size(); ++i) {
            std::cout << "report of thread-" << (i + 1) << std::endl;
            auto res = make_result(std::string{base_name(spec.base_)}, args[i], {tx[i]}, {bc[i]},
                results[i].elapsed_ms_, results[i].operations_per_tx_, std::nullopt);
            res.histogram_ = histograms[i];
            reporter_.report(res, args[i].output_path_, false);
            total.add(histograms[i]);
        }
        auto elapsed = static_cast<std::int64_t>(std::floor(sum_elapsed / static_cast<double>(results.size()) + 0.5));
        auto res = make_result(spec.name_, args[0], tx, bc, elapsed, results[0].operations_per_tx_, static_cast<std::int64_t>(results.size()));
        res.histogram_ = total;
        reporter_.report(res, FLAGS_output, true);
    }

    bool execute_ddl(service_client& client, std::string const& sql) {
        std::cout << sql << std::endl;
        jogasaki::api::transaction_handle tx{};
        begin_options opts{};
        if (auto res = client.begin(opts, tx); ! res) {
            report_error("begin", res);
            return false;
        }
        if (auto res = client.execute_statement(tx, sql); ! res) {
            report_error(sql, res);
            (void) client.rollback(tx);
            return false;
        }
        if (auto res = client.commit(tx); ! res) {
            report_error("commit", res);
            return false;
        }
        return true;
    }

    // drop and create tables. DDL is executed by OCC regardless of --transaction.
    bool prepare() {
        auto client = create_client();
        for(std::int64_t id = 1; id <= FLAGS_tables; ++id) {
            if (! execute_ddl(client, entity_.drop_table_sql(id))) {
                return false;
            }
            if (! execute_ddl(client, entity_.create_table_sql(id, ! FLAGS_without_pk))) {
                return false;
            }
            std::string_view rest{FLAGS_index};
            while(! rest.empty()) {
                auto pos = rest.find(',');
                auto column = rest.substr(0, pos);
                if (! column.empty() && ! execute_ddl(client, entity_.create_index_sql(id, column))) {
                    return false;
                }
                rest = pos == std::string_view::npos ? std::string_view{} : rest.substr(pos + 1);
            }
        }
        return true;
    }

    // run one transaction with the body, and record the latencies
    template <class Body>
    bool transaction(
        service_client& client,
        begin_options const& opts,
        scenario_result& out,
        bool record_bc,
        Body&& body
    ) {
        auto begin = clock::now();
        jogasaki::api::transaction_handle tx{};
        if (auto res = client.begin(opts, tx); ! res) {
            report_error("begin", res);
            return false;
        }
        if (! body(tx)) {
            (void) client.rollback(tx);
            return false;
        }
        if (record_bc) {
            out.bc_.emplace_back(std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - begin).count());
        }
        if (auto res = client.commit(tx); ! res) {
            report_error("commit", res);
            return false;
        }
        out.tx_.emplace_back(std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - begin).count());
        return true;
    }

    bool prepare_statement(
        service_client& client,
        std::string const& sql,
        placeholders const& ph,
        std::uint64_t& stmt
    ) {
        if (auto res = client.prepare(sql, ph, stmt); ! res) {
            report_error("prepare '" + sql + "'", res);
            return false;
        }
        return true;
    }

    static bool execute(service_client& client, jogasaki::api::transaction_handle tx, std::uint64_t stmt, parameters const& ps) {
        if (auto res = client.execute_prepared_statement(tx, stmt, ps); ! res) {
            report_error("execute statement", res);
            return false;
        }
        return true;
    }

    static bool query(service_client& client, jogasaki::api::transaction_handle tx, std::uint64_t stmt, parameters const& ps) {
        if (auto res = client.execute_prepared_query(tx, stmt, ps, [](std::string_view) {}); ! res) {
            report_error("execute query", res);
            return false;
        }
        return true;
    }

    void run_scenario(scenario_arg arg, scenario_result& out) {
        auto begin = clock::now();
        out.success_ = run_scenario_body(arg, out);
        out.elapsed_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - begin).count();
        if (! out.success_) {
            stop_ = true;
        }
    }

    bool run_scenario_body(scenario_arg& arg, scenario_result& out) {
        // same as sbench: 0 operations means 100 for a quick check, or unlimited with duration
        if (arg.base_ == base_command::remove && arg.scan_range_ <= 0) {
            arg.operations_ = 1;
            arg.operations_per_tx_ = 1;
        }
        if (arg.operations_ <= 0) {
            arg.operations_ = FLAGS_duration <= 0 ? 100 : 1000000000;
        }
        if (arg.operations_per_tx_ <= 0) {
            arg.operations_per_tx_ = arg.operations_;
        }
        if (arg.base_ == base_command::noptx) {
            arg.operations_per_tx_ = 1;
        }
        if (arg.max_value_ <= 0) {
            arg.max_value_ = arg.operations_;
        }
        out.operations_per_tx_ = arg.operations_per_tx_;
        out.tx_.reserve(static_cast<std::size_t>(std::min(arg.operations_ / arg.operations_per_tx_ + 1, static_cast<std::int64_t>(1000000))));
        out.bc_.reserve(out.tx_.capacity());

        auto client = create_client();
        std::optional<clock::time_point> deadline{};
        if (FLAGS_duration > 0) {
            deadline = clock::now() + std::chrono::seconds{FLAGS_duration};
        }
        auto over = [&]() {
            return stop_.load() || (deadline && clock::now() >= *deadline);
        };
        auto opts = tx_options(arg.table_id_);
        random_engine rnd{std::random_device{}()};
        auto ops = arg.operations_;
        auto per_tx = arg.operations_per_tx_;
        auto table = arg.table_id_;

        if (arg.base_ == base_command::noptx) {
            for(std::int64_t i = 0; i < ops; ++i) {
                if (! transaction(client, opts, out, false, [](auto) { return true; })) {
                    return false;
                }
                if (over()) {
                    break;
                }
            }
            return true;
        }

        std::string sql{};
        placeholders ph{};
        bool is_query = false;
        switch(arg.base_) {
            case base_command::insert: sql = entity_.insert_sql(table, upsert_); ph = entity_.row_placeholders(); break;
            case base_command::update: sql = entity_.update_sql(table); ph = entity_.row_placeholders(); break;
            case base_command::select: {
                is_query = true;
                auto columns = replace_all(FLAGS_select_columns, ",", " ");
                if (arg.scan_range_ <= 0) {
                    sql = entity_.select_full_scan_sql(table, columns);
                } else if (arg.scan_range_ == 1) {
                    sql = entity_.select_point_sql(table, columns, FLAGS_condition_column);
                    ph = entity_.point_placeholders();
                } else {
                    sql = entity_.select_range_sql(table, columns, FLAGS_condition_column);
                    ph = entity_.range_placeholders();
                }
                break;
            }
            case base_command::remove:
                if (arg.scan_range_ <= 0) {
                    sql = entity_.delete_all_sql(table);
                } else if (arg.scan_range_ == 1) {
                    sql = entity_.delete_point_sql(table);
                    ph = entity_.point_placeholders();
                } else {
                    sql = entity_.delete_scan_sql(table);
                    ph = entity_.range_placeholders();
                }
                break;
            default: std::abort();
        }
        std::uint64_t stmt{};
        if (! prepare_statement(client, sql, ph, stmt)) {
            return false;
        }
        bool success = true;
        for(std::int64_t i = 0; i < ops; i += per_tx) {
            std::vector<parameters> params{};
            params.reserve(static_cast<std::size_t>(per_tx));
            // parameters are generated before starting the transaction except for select, same as sbench
            switch(arg.base_) {
                case base_command::insert:
                case base_command::update: {
                    auto origin = arg.id_origin_ + i;
                    for(std::int64_t id = origin; id < origin + per_tx; ++id) {
                        params.emplace_back(entity_.row(id, origin, origin + per_tx, rnd));
                    }
                    break;
                }
                case base_command::remove:
                    for(std::int64_t j = 0; j < per_tx; ++j) {
                        if (arg.scan_range_ <= 0) {
                            params.emplace_back();
                        } else if (arg.scan_range_ == 1) {
                            params.emplace_back(entity_.point(arg.id_origin_ + i + j));
                        } else {
                            // same as sbench, the range is shifted by i * scan_range
                            auto from = arg.id_origin_ + (i * arg.scan_range_) + (j * arg.scan_range_);
                            params.emplace_back(entity_.range(from, from + arg.scan_range_));
                        }
                    }
                    break;
                default: break;
            }
            success = transaction(client, opts, out, true, [&](jogasaki::api::transaction_handle tx) {
                if (! is_query) {
                    for(auto&& p : params) {
                        if (! execute(client, tx, stmt, p)) {
                            return false;
                        }
                    }
                    return true;
                }
                for(std::int64_t j = 0; j < per_tx; ++j) {
                    parameters p{};
                    if (arg.scan_range_ == 1) {
                        p = entity_.point(random_between(arg.id_origin_, arg.id_origin_ + arg.max_value_, rnd));
                    } else if (arg.scan_range_ >= 2) {
                        auto from = random_between(arg.id_origin_, arg.id_origin_ + arg.max_value_ - arg.scan_range_, rnd);
                        p = entity_.range(from, from + arg.scan_range_);
                    }
                    if (! query(client, tx, stmt, p)) {
                        return false;
                    }
                }
                return true;
            });
            if (! success) {
                break;
            }
            if (over()) {
                break;
            }
            if (arg.base_ == base_command::insert && FLAGS_interval > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds{FLAGS_interval});
            }
        }
        (void) client.dispose_statement(stmt);
        return success;
    }
};

/**
 * @brief convert sbench style arguments to the gflags style
 * @details short options (-c, -o, -d, -i, -t, -e, -h) are converted to long ones, hyphens in option names are
 * replaced with underscores, and the values of boolean options given as separate arguments are merged
 */
std::vector<std::string> convert_arguments(int argc, char* argv[]) {  //NOLINT
    static std::map<std::string, std::string, std::less<>> const short_options{
        {"-c", "--connection"},
        {"-o", "--output"},
        {"-d", "--duration"},
        {"-i", "--interval"},
        {"-t", "--transaction"},
        {"-e", "--entity"},
        {"-h", "--header"},
    };
    std::vector<std::string> ret{};
    for(int i = 0; i < argc; ++i) {
        std::string a{argv[i]};  //NOLINT
        if (i == 0 || a.size() < 2 || a[0] != '-') {
            ret.emplace_back(std::move(a));
            continue;
        }
        if (auto it = short_options.find(a); it != short_options.end()) {
            a = it->second;
        }
        if (a.rfind("--", 0) == 0) {
            auto eq = a.find('=');
            auto name_end = eq == std::string::npos ? a.size() : eq;
            std::replace(a.begin() + 2, a.begin() + static_cast<std::ptrdiff_t>(name_end), '-', '_');
            if (eq == std::string::npos && (a == "--without_pk" || a == "--auto_inc" || a == "--header") && i + 1 < argc) {
                std::string next{argv[i + 1]};  //NOLINT
                if (next == "true" || next == "false") {
                    a += "=" + next;
                    ++i;
                }
            }
        }
        ret.emplace_back(std::move(a));
    }
    return ret;
}

}  // namespace tateyama::nsbench

extern "C" int main(int argc, char* argv[]) {
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("nsbench");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage(
        "nsbench - simple-benchmark (sbench) running on in-process jogasaki\n"
        "usage: nsbench <command>... [options]\n"
        "commands: prepare, insert, insert-st, insert-mt, update, update-st, update-mt, select, select-st, select-mt,\n"
        "          delete, delete-mt, noptx, noptx-st\n"
        "Multiple commands are executed in order on the same database, e.g. `nsbench prepare insert update`.");
    // suppress engine info logs mixed into the results by default
    jogasaki::common_cli::set_flag_defaults({{"minloglevel", "1"}});
    auto converted = tateyama::nsbench::convert_arguments(argc, argv);
    std::vector<char*> args{};
    for(auto&& a : converted) {
        args.emplace_back(a.data());
    }
    int n = static_cast<int>(args.size());
    char** p = args.data();
    gflags::ParseCommandLineFlags(&n, &p, true);

    std::vector<tateyama::nsbench::command_spec> specs{};
    for(int i = 1; i < n; ++i) {
        auto const& cmds = tateyama::nsbench::commands();
        auto it = cmds.find(std::string_view{p[i]});  //NOLINT
        if (it == cmds.end()) {
            LOG(ERROR) << "unsupported command: " << p[i];  //NOLINT
            return -1;
        }
        specs.emplace_back(it->second);
    }
    if (specs.empty()) {
        gflags::ShowUsageWithFlagsRestrict(argv[0], "nsbench");  //NOLINT
        return -1;
    }
    auto cfg = std::make_shared<jogasaki::configuration>();
    tateyama::nsbench::nsbench b{};
    if (! b.init(*cfg)) {
        return -1;
    }
    try {
        return b.run(cfg, specs);
    } catch (std::exception& e) {
        LOG(ERROR) << e.what();
        return -1;
    }
}
