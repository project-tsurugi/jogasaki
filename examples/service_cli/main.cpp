/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cxxabi.h>
#include <decimal.hh>
#include <errno.h>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <iterator>
#include <linenoise.h>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <tateyama/api/configuration.h>
#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/proto/diagnostics.pb.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/tables.h>

#include "../common/temporary_folder.h"
#include "../common/utils.h"

DEFINE_bool(single_thread, false, "Whether to run on serial scheduler");  //NOLINT
DEFINE_int32(thread_count, 1, "Number of threads");  //NOLINT
DEFINE_bool(core_affinity, false, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(explain, false, "explain the execution plan");  //NOLINT
DEFINE_int32(partitions, 10, "Number of partitions per process");  //NOLINT
DEFINE_bool(steal, false, "Enable stealing for task scheduling");  //NOLINT
DEFINE_bool(auto_commit, true, "Whether to commit when finishing each statement.");  //NOLINT
DEFINE_int32(prepare_data, 0, "Whether to prepare records in the storages. Specify 0 to disable.");  //NOLINT
DEFINE_bool(verify_record, true, "Whether to deserialize the query result records");  //NOLINT
DEFINE_bool(test_build, false, "To verify build of this executable");  //NOLINT
DEFINE_string(location, "TMP", "specify the database directory. Pass TMP to use temporary directory.");  //NOLINT
DEFINE_string(history_file, ".service_cli_history", "specify the command history file name");  //NOLINT
DEFINE_int32(exit_on_idle, 180, "Exit the program if user leaves the command line idle. Specify the duration in second, or -1 not to exit.");  //NOLINT
DEFINE_string(input_file, "", "specify the input commands file to read and execute");  //NOLINT
DEFINE_string(load_from, "", "specify the generated db file directory. Use to prepare initial data.");  //NOLINT
DECLARE_int32(dump_batch_size);  //NOLINT
DECLARE_int32(load_batch_size);  //NOLINT

namespace tateyama::service_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using takatori::util::unsafe_downcast;

namespace sql = jogasaki::proto::sql;

struct stmt_info {
    stmt_info() = default;
    stmt_info(std::string_view sql, std::unordered_map<std::string, sql::common::AtomType> vars) :
        sql_(sql),
        host_variables_(std::move(vars))
    {}
    std::string sql_{};
    std::unordered_map<std::string, sql::common::AtomType> host_variables_{};
};

class cli {
    takatori::util::maybe_shared_ptr<jogasaki::api::database> db_{};
    std::shared_ptr<jogasaki::api::impl::service> service_{};  //NOLINT
    jogasaki::api::transaction_handle tx_handle_{};  //NOLINT
    bool tx_processing_{};  //NOLINT
    bool debug_{}; //NOLINT
    bool verify_query_records_{};
    bool auto_commit_{};
    std::mutex write_buffer_mutex_{};
    std::stringstream write_buffer_{};
    std::vector<std::pair<std::uint64_t, stmt_info>> stmt_handles_{};
    std::vector<std::future<bool>> on_going_statements_{};
    jogasaki::meta::record_meta query_meta_{};
    jogasaki::common_cli::temporary_folder temporary_{};
    std::map<std::string, sql::common::AtomType> host_variables_{};
    std::int32_t exit_on_idle_{-1};
    std::ifstream input_file_stream_{};

    using Clock = std::chrono::system_clock;
    std::atomic<Clock::time_point> last_interacted_{Clock::now()};
    std::atomic_bool to_exit_{false};
    std::string load_from_{};

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
                std::cerr << "parsing options failed" << std::endl;
            }
        }
        cfg.single_thread(FLAGS_single_thread);
        cfg.thread_pool_size(FLAGS_thread_count);

        cfg.core_affinity(FLAGS_core_affinity);
        cfg.initial_core(FLAGS_initial_core);
        cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
        cfg.default_partitions(FLAGS_partitions);
        cfg.stealing_enabled(FLAGS_steal);

        if (FLAGS_test_build) {
            cfg.thread_pool_size(1);
            cfg.initial_core(1);
            cfg.core_affinity(false);
            cfg.default_partitions(1);
        }

        if (FLAGS_location == "TMP") {
            temporary_.prepare();
            cfg.db_location(temporary_.path());
        } else {
            cfg.db_location(std::string(FLAGS_location));
        }
        return true;
    }

    void update_timestamp() {
        last_interacted_.store(Clock::now());
    }

    int run(std::shared_ptr<jogasaki::configuration> cfg) {
        cfg->skip_smv_check(true);  // skip strict version check for internal use
        db_ = std::shared_ptr{jogasaki::api::create_database(cfg)};
        auto c = std::make_shared<tateyama::api::configuration::whole>("");
        service_ = std::make_shared<jogasaki::api::impl::service>(c, db_.get());
        db_->start();
        auto& impl = jogasaki::api::impl::get_impl(*db_);
        jogasaki::utils::add_benchmark_tables(*impl.tables());
        jogasaki::executor::register_kvs_storage(*impl.kvs_db(), *impl.tables());

        debug_ = FLAGS_debug;
        verify_query_records_ = FLAGS_verify_record;
        auto_commit_ = FLAGS_auto_commit;
        exit_on_idle_ = FLAGS_exit_on_idle;
        load_from_ = FLAGS_load_from;

        jogasaki::executor::register_kvs_storage(*impl.kvs_db(), *impl.tables());
        if (! load_from_.empty()) {
            jogasaki::common_cli::load(*db_, load_from_);
        }
        if (FLAGS_prepare_data > 0) {
            prepare_data(*db_, FLAGS_prepare_data);
        }

        if (FLAGS_test_build) {
            to_exit_ = true;
        }

        // thread to terminate execution after idle duration
        auto f = std::async(std::launch::async, [&](){
            if (exit_on_idle_ > 0) {
                while(! to_exit_) {
                    std::this_thread::sleep_for(10ms);
                    auto now = Clock::now();
                    if (std::chrono::duration_cast<std::chrono::seconds>(now - last_interacted_.load()).count() > exit_on_idle_) {
                        std::cerr << std::endl << "Program exits because no interaction has been made for " << exit_on_idle_ << " secs. " << std::endl;
                        std::exit(1);
                    }
                }
            }
        });

        linenoiseHistoryLoad(FLAGS_history_file.c_str());
        if (! FLAGS_input_file.empty()) {
            boost::filesystem::path path{FLAGS_input_file};
            if(! boost::filesystem::exists(path)) {
                std::cerr << "Specified file not found : " << path << std::endl;
            } else {
                input_file_stream_.open(path.c_str());
            }
        }
        std::string input{};
        while(! to_exit_) {
            if (input_file_stream_.is_open()) {
                std::getline(input_file_stream_, input);
                if (input.empty()) {
                    input_file_stream_.close();
                    continue;
                }
                std::cout << "> " << input << std::endl;
            } else {
                update_timestamp();
                auto* l = linenoise("> ");
                update_timestamp();
                if (! l) continue;
                linenoiseHistoryAdd(l);
                linenoiseHistorySave(FLAGS_history_file.c_str());
                input = l;
                free(l);  //NOLINT
            }
            auto args = split(input);
            if(args.empty() || args[0].empty()) {
                print_usage();
                continue;
            }
            char cmd = args[0][0];
            args.erase(args.begin());
            switch (cmd) {
                case 'b':
                    begin_tx();
                    break;
                case 'c':
                    commit_tx();
                    break;
                case 'a':
                    abort_tx();
                    break;
                case 'p':
                    prepare(args);
                    break;
                case 'l':
                    list_statements();
                    break;
                case 'q':
                    issue_query(args);
                    break;
                case 's':
                    issue_statement(args);
                    break;
                case 'e':
                    to_exit_ = true;
                    break;
                case '#':  // comment line
                    break;
                case 'v':
                    register_variables(args);
                    break;
                case 'w':
                    wait_for(args);
                    break;
                case 'x':
                    explain_statement(args);
                    break;
                case 'h':
                default:
                    print_usage();
                    break;
            }
        }

        db_->stop();
        return 0;
    }

private:
    std::vector<std::string_view> split(std::string_view in, char delim = ' ', char quote='"') {
        std::vector<std::string_view> ret{};
        auto it = in.begin();
        auto prev = in.begin();
        enum class state {
            nothing_read,
            partially_read,
            in_quote,
            end,
            error,
        } st{};
        st = state::nothing_read;
        bool run = true;
        while(run) {
            switch(st) {
                case state::nothing_read:
                    if (it == in.end()) {
                        st = state::end;
                        break;
                    }
                    if (*it == quote) {
                        ++it;
                        prev = it;
                        st = state::in_quote;
                        break;
                    }
                    if (*it == delim) {
                        ++it;
                        prev = it;
                        break;
                    }
                    prev = it;
                    ++it;
                    st = state::partially_read;
                    break;
                case state::partially_read:
                    if (it == in.end()) {
                        st = state::end;
                        break;
                    }
                    if (*it == quote) {
                        std::cerr << "met quote in partially reading" << std::endl;
                        st = state::error;
                        break;
                    }
                    if (*it == delim) {
                        ret.emplace_back(prev, std::distance(prev, it));
                        ++it;
                        prev = it;
                        st = state::nothing_read;
                        break;
                    }
                    ++it;
                    break;
                case state::in_quote:
                    if (it == in.end()) {
                        std::cerr << "saw eof in quote" << std::endl;
                        st = state::error;
                        break;
                    }
                    if (*it == quote) {
                        if (prev != it) {
                            ret.emplace_back(prev, std::distance(prev, it));
                        }
                        ++it;
                        prev = it;
                        st = state::nothing_read;
                        break;
                    }
                    ++it; // delimiter is same as other characters
                    break;
                case state::end:
                    if (prev != it) {
                        ret.emplace_back(prev, std::distance(prev, it));
                    }
                    run = false;
                    break;
                case state::error:
                    ret.clear();
                    run = false;
                    break;
            }
        }
        if(FLAGS_debug) {
            std::cout << "ret:" << std::endl;
            int i = 0;
            for(auto x : ret) {
                std::cout << i << " : '" << x << "'" << std::endl;
                ++i;
            }
        }
        return ret;
    }
    void print_usage() {
        std::cout <<
            "command: " << std::endl <<
            "  a : abort transaction" << std::endl <<
            "  b : begin transaction" << std::endl <<
            "  c : commit transaction" << std::endl <<
            "  e : exit" << std::endl <<
            "  l : list prepared statements" << std::endl <<
            "  p <sql text> : prepare statement" << std::endl <<
            "  q <query text or number> : issue query " << std::endl <<
            "  s <statement text or number> : issue statement " << std::endl <<
            "  v [<name>:<type>] : show or register host variables" << std::endl <<
            "  w [<duration millisecond>] : wait for the given duration(ms)" << std::endl <<
            "  x <statement number> : explain query/statement " << std::endl <<
            "";
    }
    bool begin_tx(bool for_autocommit = false) {
        if (tx_processing_) {
            std::cout << "command was ignored. transaction already started: " << tx_handle_ << std::endl;
            return false;
        }
        auto s = jogasaki::utils::encode_begin(false);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        if(! wait_response(*res)) {
            LOG(ERROR) << "response timed out";
            return false;
        }
        bool error{false};
        if(! st || res->error_.code() != ::tateyama::proto::diagnostics::Code::UNKNOWN) {
            std::cerr << "error executing command" << std::endl;
            error = true;
        }
        auto [h, id] = jogasaki::utils::decode_begin(res->body_);
        tx_handle_ = h;
        (void) id;
        if(error) return false;
        if(! for_autocommit) {  // suppress msg if auto commit
            std::cout << "transaction begin: " << tx_handle_ << std::endl;
        }
        tx_processing_ = true;
        return true;
    }
    bool handle_result_only(std::string_view body, bool suppress_msg = false) {
        auto [success, error] = jogasaki::utils::decode_result_only(body);
        if (success) {
            if (! suppress_msg) {
                std::cout << "command completed successfully." << std::endl;
            }
            return true;
        }
        std::cerr << "command returned " << error.code_ << ": " << error.message_ << std::endl;
        return false;
    }

    // block wait for async query execution
    void wait_for_statements() {
        for(auto&& f : on_going_statements_) {
            (void)f.get();
        }
        on_going_statements_.clear();
    }
    bool commit_tx(bool for_autocommit = false) {
        if (! tx_processing_) {
            std::cout << "command was ignored. no transaction started yet" << std::endl;
            return false;
        }
        if(for_autocommit) {
            wait_for_statements(); // just for cleanup
        }
        auto s = jogasaki::utils::encode_commit(tx_handle_, true);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        if(! wait_response(*res)) {
            LOG(ERROR) << "response timed out";
            return false;
        }
        if(! st || res->error_.code() != ::tateyama::proto::diagnostics::Code::UNKNOWN) {
            std::cerr << "error executing command" << std::endl;
        }
        auto ret = handle_result_only(res->body_, for_autocommit);
        tx_processing_ = false; // tx is finished and destroyed even on failure
        if (ret) {
            tx_handle_ = jogasaki::api::transaction_handle{1};
        }
        wait_for_statements(); // just for cleanup
        return ret;
    }
    bool wait_response(tateyama::api::server::mock::test_response& res) {
        std::size_t cnt = 0;
        constexpr std::size_t max_wait = 100;
        while(! res.completed() && cnt < max_wait) {
            std::this_thread::sleep_for(1ms);
            ++cnt;
        }
        return cnt < max_wait;
    }
    bool abort_tx() {
        if (! tx_processing_) {
            std::cout << "command was ignored. no transaction started yet" << std::endl;
            return false;
        }
        wait_for_statements();
        auto s = jogasaki::utils::encode_rollback(tx_handle_);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        if(! wait_response(*res)) {
            LOG(ERROR) << "response timed out";
            return false;
        }
        if(! st || res->error_.code() != ::tateyama::proto::diagnostics::Code::UNKNOWN) {
            std::cerr << "error executing command" << std::endl;
        }
        auto ret = handle_result_only(res->body_);
        tx_processing_ = false; // tx is finished and destroyed even on failure
        if (ret) {
            tx_handle_ = jogasaki::api::transaction_handle{1};
        }
        wait_for_statements(); // just for cleanup
        return ret;
    }
    sql::common::AtomType from(std::string_view str) {
        static const std::unordered_map<std::string, sql::common::AtomType> map{
            {"int4", sql::common::AtomType::INT4},
            {"int8", sql::common::AtomType::INT8},
            {"float4", sql::common::AtomType::FLOAT4},
            {"float8", sql::common::AtomType::FLOAT8},
            {"character", sql::common::AtomType::CHARACTER},
            {"date", sql::common::AtomType::DATE},
            {"time_of_day", sql::common::AtomType::TIME_OF_DAY},
            {"time_point", sql::common::AtomType::TIME_POINT},
            {"decimal", sql::common::AtomType::DECIMAL},

            {"i4", sql::common::AtomType::INT4},
            {"i8", sql::common::AtomType::INT8},
            {"f4", sql::common::AtomType::FLOAT4},
            {"f8", sql::common::AtomType::FLOAT8},
            {"ch", sql::common::AtomType::CHARACTER},
            {"dt", sql::common::AtomType::DATE},
            {"td", sql::common::AtomType::TIME_OF_DAY},
            {"tp", sql::common::AtomType::TIME_POINT},
            {"dec", sql::common::AtomType::DECIMAL},
        };

        if(map.count(std::string(str)) != 0) {
            return map.at(std::string(str));
        }
        return sql::common::AtomType::TYPE_UNSPECIFIED; //unsupported type
    }

    bool is_alphanumeric(char c) {
        return c == '_' || (c >= '0' && c <='9') || (c >= 'a' && c <='z') || (c >= 'A' && c <='Z');
    }
    std::vector<std::string_view> extract_variable_names(std::string_view sql) {
        // the variables name is the longest string composed by alphanumeric letters and underscore after colon
        std::vector<std::string_view> ret{};
        std::size_t pos = 0;
        std::size_t prev = 0;

        while(true) {
            pos = sql.find(':', prev);
            if(pos == std::string::npos) {
                break;
            }
            ++pos;
            prev = pos;
            while(true) {
                if(! is_alphanumeric(*(sql.data()+pos))) {  //NOLINT
                    break;
                }
                ++pos;
            }
            if(prev != pos) {
                ret.emplace_back(sql.data()+prev, pos-prev);  //NOLINT
            }
            prev = pos;
        }
        if (debug_) {
            int cnt = 0;
            std::cout << "extracted variable names" << std::endl;
            for(auto&& a : ret) {
                std::cout << "  " << cnt << " : " << a << std::endl;
                ++cnt;
            }
        }
        return ret;
    }

    std::vector<std::string_view> split_string(std::string_view s, char delim) {
        std::vector<std::string_view> ret{};
        std::size_t pos = 0;
        std::size_t prev = 0;
        while(true) {
            pos = s.find(delim, prev);
            if(pos == std::string::npos) {
                if (prev != s.size()) {
                    ret.emplace_back(s.data()+prev, s.size()-prev);  //NOLINT
                }
                break;
            }
            if (pos > prev) {
                ret.emplace_back(s.data()+prev, pos-prev);  //NOLINT
            }
            ++pos;
            prev = pos;
        }
        if (debug_) {
            int cnt = 0;
            std::cout << "split string" << std::endl;
            for(auto&& a : ret) {
                std::cout << "  " << cnt << " : " << a << std::endl;
                ++cnt;
            }
        }
        return ret;
    }

    bool wait_for(std::vector<std::string_view> const& args) {
        std::size_t dur_ms{};
        if (! args.empty() && ! args[0].empty()) {
            std::string text{};
            std::int32_t idx{};
            if (! fill_text_or_index(args[0], text, idx)) {
                std::cerr << "parsing variable failed : " << args[0] << std::endl;
                return false;
            }
            dur_ms = idx;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{dur_ms});
        return true;
    }

    bool register_variables(std::vector<std::string_view> const& args) {
        if (args.empty()) {
            std::cout << "list host variables" << std::endl;
            for(auto&& v : host_variables_) {
                std::cout << v.first << " : " << sql::common::AtomType_Name(v.second) << std::endl;
            }
            return true;
        }
        for(auto&& s : args) {
            auto var = split_string(s, ':');
            if (var.size() != 2) {
                std::cerr << "parsing variable failed : " << s << std::endl;
                return false;
            }
            auto t = from(var[1]);
            if(t == sql::common::AtomType::TYPE_UNSPECIFIED) {
                std::cerr << "type is not supported : " << s << std::endl;
                return false;
            }
            host_variables_.emplace(var[0], t);
        }
        return true;
    }
    bool prepare(std::vector<std::string_view> const& args) {
        if (args.size() > 1) {
            std::cout << "command was ignored. too many command args" << std::endl;
            return false;
        }
        std::string sql{args[0]};
        auto vars = extract_variable_names(sql);
        std::unordered_map<std::string, sql::common::AtomType> types{};
        for(auto&& v: vars) {
            if(host_variables_.count(std::string{v}) != 0) {
                types[std::string{v}] = host_variables_.at(std::string{v});
            }
        }
        auto s = jogasaki::utils::encode_prepare_vars(sql, types);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        bool error{false};
        if(! wait_response(*res)) {
            LOG(ERROR) << "response timed out";
            return false;
        }
        if(! st || res->error_.code() != ::tateyama::proto::diagnostics::Code::UNKNOWN) {
            std::cerr << "error executing command" << std::endl;
            error = true;
        }
        auto stmt_handle = jogasaki::utils::decode_prepare(res->body_);
        if (error) return false;
        std::cout << "statement #" << stmt_handles_.size() << " prepared: " << stmt_handle << std::endl;
        stmt_handles_.emplace_back(stmt_handle, stmt_info{sql, std::move(types)});
        return true;
    }
    bool list_statements() {
        std::size_t i{};
        if (stmt_handles_.empty()) {
            std::cout << "no entry" << std::endl;
            return true;
        }
        for(auto&& [handle, info] : stmt_handles_) {
            std::cout << "#" << i << " " << handle << " : " << info.sql_ << std::endl;
            ++i;
        }
        return true;
    }

    template<class T>
    T to_value(std::string_view val) {
        if constexpr (std::is_same_v<T, std::int32_t> || std::is_same_v<T, std::int64_t>) {  //NOLINT
            return std::strtol(val.data(), nullptr, 10);
        } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {  //NOLINT
            return std::strtod(val.data(), nullptr);
        } else if constexpr (std::is_same_v<T, std::string>) {  //NOLINT
            return std::string{val};
        } else if constexpr (std::is_same_v<T, takatori::decimal::triple>) {  //NOLINT
            decimal::Decimal d{std::string{val}};
            return takatori::decimal::triple{d.as_uint128_triple()};
        } else {
            std::abort();
        }
        return {};
    }
    bool parse_parameters(
        std::int32_t stmt_idx,
        std::string_view sql,
        std::vector<std::string_view> const& args,
        std::vector<jogasaki::utils::parameter>& parameters
    ) {
        stmt_info info{};
        if(stmt_idx >= 0 && static_cast<std::size_t>(stmt_idx) >= stmt_handles_.size()) {
            std::cerr << "invalid index" << std::endl;
            return false;
        }
        info = stmt_idx >= 0 ? stmt_handles_[stmt_idx].second : stmt_info{sql, {}};
        auto& types = info.host_variables_;
        std::size_t used = 0;
        for(auto s : args) {
            auto v = split_string(s, '=');
            if (v.size() != 2) {
                std::cerr << "invalid parameter : " << s << std::endl;
                return false;
            }
            std::string name{v[0]};
            if (types.count(name) == 0) {
                std::cerr << "Host variable('" << name << "') value is assigned but not used in statement : " << info.sql_ << std::endl;
                return false;
            }
            ++used;
            auto val = v[1];
            if(val == "NULL" || val == "null" || val == "Null") {
                parameters.emplace_back(name);
                continue;
            }
            auto type = types.at(name);
            switch (type) {
                using ValueCase = sql::request::Parameter::ValueCase;
                case sql::common::AtomType::INT4: parameters.emplace_back(name, ValueCase::kInt4Value, to_value<std::int32_t>(val)); break;
                case sql::common::AtomType::INT8: parameters.emplace_back(name, ValueCase::kInt8Value, to_value<std::int64_t>(val)); break;
                case sql::common::AtomType::FLOAT4: parameters.emplace_back(name, ValueCase::kFloat4Value, to_value<float>(val)); break;
                case sql::common::AtomType::FLOAT8: parameters.emplace_back(name, ValueCase::kFloat8Value, to_value<double>(val)); break;
                case sql::common::AtomType::CHARACTER: parameters.emplace_back(name, ValueCase::kCharacterValue, to_value<std::string>(val)); break;
                case sql::common::AtomType::DATE: parameters.emplace_back(name, ValueCase::kDateValue, to_value<takatori::datetime::time_of_day>(val)); break;
                case sql::common::AtomType::TIME_OF_DAY: parameters.emplace_back(name, ValueCase::kTimeOfDayValue, to_value<takatori::datetime::time_of_day>(val)); break;
                case sql::common::AtomType::TIME_POINT: parameters.emplace_back(name, ValueCase::kTimePointValue, to_value<takatori::datetime::time_point>(val)); break;
                case sql::common::AtomType::DECIMAL: parameters.emplace_back(name, ValueCase::kDecimalValue, to_value<takatori::decimal::triple>(val)); break;
                default:
                    std::cerr << "invalid type" << std::endl;
                    return false;
            }
        }
        if (used < types.size()) {
            std::cerr << "Not all host variable values are assigned for statement : " << info.sql_ << std::endl;
            return false;
        }
        return true;
    }

    // return true if numeric index is extracted, otherwise text is filled
    bool fill_text_or_index(std::string_view arg, std::string& text, std::int32_t& idx) {
        auto b = arg.data();
        char* e{};
        text.clear();
        idx = std::strtol(b, &e, 10);
        bool ret = true;
        if (b == e || errno == ERANGE) {
            ret = false;
            text = arg;
            idx = -1;
        }
        return ret;
    }

    void reset_write_buffer() {
        write_buffer_.str("");
        write_buffer_.clear();
    }

    bool issue_common(
        bool query,
        std::vector<std::string_view>& args,
        std::function<void(std::string_view)> on_write
    ) {
        if (args.empty()) {
            std::cout << "command was ignored. missing command args" << std::endl;
            return false;
        }
        if (! tx_processing_) {
            if (! auto_commit_) {
                std::cout << "command was ignored. no transaction started yet" << std::endl;
                return false;
            }
            if (! begin_tx(true)) {
                std::cout << "auto commit begin failed." << std::endl;
                return false;
            }
        }
        std::string sql{};
        std::int32_t idx{};
        if (fill_text_or_index(args[0], sql, idx) &&
            (idx < 0 || static_cast<std::size_t>(idx) >= stmt_handles_.size())) {
            std::cerr << "statement index (" << idx << ") is out of range" << std::endl;
            return false;
        }

        args.erase(args.begin());
        std::vector<jogasaki::utils::parameter> parameters{};
        if(! parse_parameters(idx, sql, args, parameters)) {
            return false;
        }
        std::string s{};
        if (! sql.empty()) {
            s = query ?
                jogasaki::utils::encode_execute_query(tx_handle_, sql) :
                jogasaki::utils::encode_execute_statement(tx_handle_, sql);
        } else {
            s = query ?
                jogasaki::utils::encode_execute_prepared_query(tx_handle_, stmt_handles_[idx].first, parameters) :
                jogasaki::utils::encode_execute_prepared_statement(tx_handle_, stmt_handles_[idx].first, parameters);
        }
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        if (query) {
            res->set_on_write(std::move(on_write));
        }
        on_going_statements_.emplace_back(
            std::async(std::launch::async, [&, res]() {
                wait_response(*res);
                if (res->error_.code() != ::tateyama::proto::diagnostics::Code::UNKNOWN) {
                    std::cerr << "error executing command" << std::endl;
                }
                if (verify_query_records_) {
                    auto recs = jogasaki::utils::deserialize_msg(
                        tateyama::api::server::mock::view_of(write_buffer_),
                        query_meta_
                    );
                    for(auto&& r : recs) {
                        std::cout << "record : " << r << std::endl;
                    }
                }
                return handle_result_only(res->body_);
            })
        );
        reset_write_buffer();
        auto st = (*service_)(req, res);
        if(! st) {
            std::cerr << "service invocation failed" << std::endl;
            return false;
        }
        if (query) {
            auto [name, columns] = jogasaki::utils::decode_execute_query(res->body_head_);
            std::cout << "query name : " << name << std::endl;
            query_meta_ = jogasaki::utils::create_record_meta(columns);
            std::size_t ind{};
            for(auto&& f : query_meta_) {
                std::cout << "column " << ind << ": " << f << " (\"" << columns[ind].name_ << "\")" << std::endl;
                ++ind;
            }
        }
        if (auto_commit_) {
            if (! commit_tx(true)) {
                std::cout << "auto commit failed to commit tx" << std::endl;
                return false;
            }
        }
        return true;
    }
    bool issue_statement(std::vector<std::string_view>& args) {
        return issue_common(false, args, [](std::string_view){});
    }
    bool issue_query(std::vector<std::string_view>& args) {
        return issue_common(true, args, [&](std::string_view data) {
            std::cout << "write: " << jogasaki::utils::binary_printer{data.data(), data.size()} << std::endl;
            if (verify_query_records_) {
                std::unique_lock lk{write_buffer_mutex_};
                write_buffer_.write(data.data(), data.size());
                write_buffer_.flush();
            }
        });
    }
    bool explain_statement(std::vector<std::string_view>& args) {
        if (args.empty()) {
            std::cout << "command was ignored. missing command args" << std::endl;
            return false;
        }
        std::string sql{};
        std::int32_t idx{};
        if (!fill_text_or_index(args[0], sql, idx) ||
            (idx < 0 || static_cast<std::size_t>(idx) >= stmt_handles_.size())) {
            std::cerr << "statement index (" << idx << ") is invalid or out of range" << std::endl;
            return false;
        }

        args.erase(args.begin());
        std::vector<jogasaki::utils::parameter> parameters{};
        if(! parse_parameters(idx, sql, args, parameters)) {
            return false;
        }
        auto s = jogasaki::utils::encode_explain(stmt_handles_[idx].first, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        if(! st) {
            std::cerr << "service invocation failed" << std::endl;
            return false;
        }

        auto [explained, id, version, cols, err] = jogasaki::utils::decode_explain(res->body_);
        if (explained.empty()) {
            std::cerr << "explain error: " << err.code_ << " " << err.message_ << std::endl;
            return false;
        }
        std::cout << explained << std::endl;
        return true;
    }

};

}  // namespace tateyama::service_cli

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("service cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("service cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    tateyama::service_cli::cli e{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    e.fill_from_flags(*cfg);
    try {
        e.run(cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
