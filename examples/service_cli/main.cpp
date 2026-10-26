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
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <decimal.hh>
#include <errno.h>
#include <exception>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <linenoise.h>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <takatori/datetime/conversion.h>
#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <tateyama/api/server/mock/request_response.h>

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/error.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/utils/msgbuf_utils.h>

#include "service_client.h"
#include "service_setup.h"

DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(auto_commit, true, "Whether to commit when finishing each statement executed outside explicitly started transaction.");  //NOLINT
DEFINE_bool(verify_record, true, "Whether to deserialize the query result records");  //NOLINT
DEFINE_bool(test_build, false, "To verify build of this executable");  //NOLINT
DEFINE_string(history_file, "", "specify the command history file name. Defaults to $HOME/.service_cli_history");  //NOLINT
DEFINE_int32(exit_on_idle, -1, "Exit the program if user leaves the command line idle. Specify the duration in second, or -1 not to exit.");  //NOLINT
DEFINE_string(input_file, "", "specify the input commands file to read and execute");  //NOLINT

namespace tateyama::service_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

namespace sql = jogasaki::proto::sql;
using jogasaki::common_cli::call_result;
using jogasaki::common_cli::service_client;

struct stmt_info {
    stmt_info() = default;
    stmt_info(std::string_view sql, std::unordered_map<std::string, sql::common::AtomType> vars) :
        sql_(sql),
        host_variables_(std::move(vars))
    {}
    std::string sql_{};
    std::unordered_map<std::string, sql::common::AtomType> host_variables_{};
};

// the session id used by this cli. Zero is avoided since it's regarded as no session.
constexpr std::size_t session_id = 1;

void print_error(call_result const& res) {
    auto& diag = res.diag();
    std::cerr << "command returned " << sql::error::Code_Name(diag.code_) << ": " << diag.message_ << std::endl;
    if (! diag.supplemental_text_.empty()) {
        std::cerr << "  supplemental text: " << diag.supplemental_text_ << std::endl;
    }
}

std::string default_history_file() {
    if (auto* home = std::getenv("HOME"); home != nullptr) {
        return (boost::filesystem::path{home} / ".service_cli_history").string();
    }
    return ".service_cli_history";
}

class cli {
    jogasaki::common_cli::service_environment env_{};
    service_client client_{};
    jogasaki::api::transaction_handle tx_handle_{};  //NOLINT
    bool tx_processing_{};  //NOLINT
    bool debug_{}; //NOLINT
    bool verify_query_records_{};
    bool auto_commit_{};
    std::mutex write_buffer_mutex_{};
    std::stringstream write_buffer_{};
    std::vector<std::pair<std::uint64_t, stmt_info>> stmt_handles_{};
    std::map<std::string, sql::common::AtomType> host_variables_{};
    std::int32_t exit_on_idle_{-1};
    std::ifstream input_file_stream_{};
    std::string history_file_{};

    using Clock = std::chrono::system_clock;
    std::atomic<Clock::time_point> last_interacted_{Clock::now()};
    std::atomic_bool to_exit_{false};

public:

    bool fill_from_flags(jogasaki::configuration& cfg) {
        if (FLAGS_test_build) {
            jogasaki::common_cli::set_flag_defaults({
                {"thread_count", "1"},
                {"initial_core", "1"},
                {"partitions", "1"},
            });
        }
        return jogasaki::common_cli::fill_configuration(cfg);
    }

    void update_timestamp() {
        last_interacted_.store(Clock::now());
    }

    int run(std::shared_ptr<jogasaki::configuration> cfg) {
        if (! env_.start(std::move(cfg))) {
            return -1;
        }
        if (! jogasaki::common_cli::setup_builtin_tables(env_.database())) {
            env_.stop();
            return -1;
        }
        client_ = service_client{env_.service(), session_id, {jogasaki::common_cli::wait_kind::sleep, {}}};

        debug_ = FLAGS_debug;
        verify_query_records_ = FLAGS_verify_record;
        auto_commit_ = FLAGS_auto_commit;
        exit_on_idle_ = FLAGS_exit_on_idle;
        history_file_ = FLAGS_history_file.empty() ? default_history_file() : FLAGS_history_file;

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

        linenoiseHistoryLoad(history_file_.c_str());
        if (! FLAGS_input_file.empty()) {
            boost::filesystem::path path{FLAGS_input_file};
            if(! boost::filesystem::exists(path)) {
                std::cerr << "Specified file not found : " << path << std::endl;
            } else {
                input_file_stream_.open(path.c_str());
            }
        }
        bool interactive = ::isatty(STDIN_FILENO) != 0;
        std::string input{};
        while(! to_exit_) {
            if (input_file_stream_.is_open()) {
                if (! std::getline(input_file_stream_, input)) {
                    input_file_stream_.close();
                    continue;
                }
                if (input.empty()) {
                    continue;
                }
                std::cout << "> " << input << std::endl;
            } else if (! interactive) {
                if (! std::getline(std::cin, input)) {
                    break;
                }
                if (input.empty()) {
                    continue;
                }
                std::cout << "> " << input << std::endl;
            } else {
                update_timestamp();
                auto* l = linenoise("> ");
                update_timestamp();
                if (! l) {
                    // EOF (Ctrl-D)
                    break;
                }
                linenoiseHistoryAdd(l);
                linenoiseHistorySave(history_file_.c_str());
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
        to_exit_ = true;
        cleanup();
        env_.stop();
        return 0;
    }

private:
    // rollback the transaction and dispose statements left on exit
    void cleanup() {
        if (tx_processing_) {
            if (auto res = client_.rollback(tx_handle_); ! res) {
                print_error(res);
            }
            tx_processing_ = false;
        }
        for(auto&& [handle, info] : stmt_handles_) {
            if (handle == jogasaki::utils::handle_undefined) {
                continue;
            }
            if (auto res = client_.dispose_statement(handle); ! res) {
                print_error(res);
            }
        }
        stmt_handles_.clear();
    }

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
        jogasaki::api::transaction_handle tx{};
        if (auto res = client_.begin({}, tx); ! res) {
            print_error(res);
            return false;
        }
        tx_handle_ = tx;
        if(! for_autocommit) {  // suppress msg if auto commit
            std::cout << "transaction begin: " << tx_handle_ << std::endl;
        }
        tx_processing_ = true;
        return true;
    }
    bool commit_tx(bool for_autocommit = false) {
        if (! tx_processing_) {
            std::cout << "command was ignored. no transaction started yet" << std::endl;
            return false;
        }
        auto res = client_.commit(tx_handle_);
        tx_processing_ = false; // tx is finished and destroyed even on failure
        tx_handle_ = {};
        if (! res) {
            print_error(res);
            return false;
        }
        if (! for_autocommit) {
            std::cout << "command completed successfully." << std::endl;
        }
        return true;
    }
    bool abort_tx() {
        if (! tx_processing_) {
            std::cout << "command was ignored. no transaction started yet" << std::endl;
            return false;
        }
        auto res = client_.rollback(tx_handle_);
        tx_processing_ = false; // tx is finished and destroyed even on failure
        tx_handle_ = {};
        if (! res) {
            print_error(res);
            return false;
        }
        std::cout << "command completed successfully." << std::endl;
        return true;
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
            while(pos < sql.size() && is_alphanumeric(sql[pos])) {
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
        if (args.empty()) {
            std::cout << "command was ignored. missing command args" << std::endl;
            return false;
        }
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
        std::uint64_t stmt_handle{};
        if (auto res = client_.prepare(sql, types, stmt_handle); ! res) {
            print_error(res);
            return false;
        }
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

    template <class T>
    bool to_value(std::string_view val, T& out) {
        if constexpr (std::is_same_v<T, std::int32_t> || std::is_same_v<T, std::int64_t>) {  //NOLINT
            out = static_cast<T>(std::strtoll(std::string{val}.c_str(), nullptr, 10));
        } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {  //NOLINT
            out = static_cast<T>(std::strtod(std::string{val}.c_str(), nullptr));
        } else if constexpr (std::is_same_v<T, std::string>) {  //NOLINT
            out = std::string{val};
        } else if constexpr (std::is_same_v<T, takatori::decimal::triple>) {  //NOLINT
            decimal::Decimal d{std::string{val}};
            out = takatori::decimal::triple{d.as_uint128_triple()};
        } else if constexpr (std::is_same_v<T, takatori::datetime::date>) {  //NOLINT
            auto res = takatori::datetime::parse_date(val);
            if (res.is_error()) {
                std::cerr << "invalid date '" << val << "' : " << res.error() << std::endl;
                return false;
            }
            auto& v = res.value();
            out = takatori::datetime::date{static_cast<std::int32_t>(v.year), v.month, v.day};
        } else if constexpr (std::is_same_v<T, takatori::datetime::time_of_day>) {  //NOLINT
            auto res = takatori::datetime::parse_time(val);
            if (res.is_error()) {
                std::cerr << "invalid time of day '" << val << "' : " << res.error() << std::endl;
                return false;
            }
            auto& v = res.value();
            out = takatori::datetime::time_of_day{v.hour, v.minute, v.second, v.subsecond};
        } else if constexpr (std::is_same_v<T, takatori::datetime::time_point>) {  //NOLINT
            auto res = takatori::datetime::parse_datetime(val);
            if (res.is_error()) {
                std::cerr << "invalid time point '" << val << "' : " << res.error() << std::endl;
                return false;
            }
            auto& v = res.value();
            if (v.offset.has_value()) {
                std::cerr << "time zone offset is not supported for time point : " << val << std::endl;
                return false;
            }
            out = takatori::datetime::time_point{
                takatori::datetime::date{static_cast<std::int32_t>(v.date.year), v.date.month, v.date.day},
                takatori::datetime::time_of_day{v.time.hour, v.time.minute, v.time.second, v.time.subsecond}
            };
        } else {
            static_assert(! std::is_same_v<T, T>, "unsupported type");
        }
        return true;
    }

    template <class T>
    bool add_parameter(
        std::vector<jogasaki::utils::parameter>& parameters,
        std::string const& name,
        sql::request::Parameter::ValueCase type,
        std::string_view val
    ) {
        T v{};
        if (! to_value(val, v)) {
            return false;
        }
        parameters.emplace_back(name, type, v);
        return true;
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
            bool success{};
            using ValueCase = sql::request::Parameter::ValueCase;
            switch (type) {
                case sql::common::AtomType::INT4: success = add_parameter<std::int32_t>(parameters, name, ValueCase::kInt4Value, val); break;
                case sql::common::AtomType::INT8: success = add_parameter<std::int64_t>(parameters, name, ValueCase::kInt8Value, val); break;
                case sql::common::AtomType::FLOAT4: success = add_parameter<float>(parameters, name, ValueCase::kFloat4Value, val); break;
                case sql::common::AtomType::FLOAT8: success = add_parameter<double>(parameters, name, ValueCase::kFloat8Value, val); break;
                case sql::common::AtomType::CHARACTER: success = add_parameter<std::string>(parameters, name, ValueCase::kCharacterValue, val); break;
                case sql::common::AtomType::DATE: success = add_parameter<takatori::datetime::date>(parameters, name, ValueCase::kDateValue, val); break;
                case sql::common::AtomType::TIME_OF_DAY: success = add_parameter<takatori::datetime::time_of_day>(parameters, name, ValueCase::kTimeOfDayValue, val); break;
                case sql::common::AtomType::TIME_POINT: success = add_parameter<takatori::datetime::time_point>(parameters, name, ValueCase::kTimePointValue, val); break;
                case sql::common::AtomType::DECIMAL: success = add_parameter<takatori::decimal::triple>(parameters, name, ValueCase::kDecimalValue, val); break;
                default:
                    std::cerr << "invalid type" << std::endl;
                    return false;
            }
            if (! success) {
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
        std::string str{arg};
        char* e{};
        text.clear();
        errno = 0;
        idx = static_cast<std::int32_t>(std::strtol(str.c_str(), &e, 10));
        if (e == str.c_str() || *e != '\0' || errno == ERANGE) {
            text = str;
            idx = -1;
            return false;
        }
        return true;
    }

    void reset_write_buffer() {
        write_buffer_.str("");
        write_buffer_.clear();
    }

    void show_query_result(jogasaki::common_cli::query_info const& info) {
        std::cout << "query name : " << info.name_ << std::endl;
        auto meta = jogasaki::utils::create_record_meta(info.columns_);
        std::size_t ind{};
        for(auto&& f : meta) {
            std::cout << "column " << ind << ": " << f << " (\"" << info.columns_[ind].name_ << "\")" << std::endl;
            ++ind;
        }
        if (verify_query_records_) {
            auto recs = jogasaki::utils::deserialize_msg(
                tateyama::api::server::mock::view_of(write_buffer_),
                meta
            );
            for(auto&& r : recs) {
                std::cout << "record : " << r << std::endl;
            }
        }
    }

    bool issue_common(
        bool query,
        std::vector<std::string_view>& args
    ) {
        if (args.empty()) {
            std::cout << "command was ignored. missing command args" << std::endl;
            return false;
        }
        if (! tx_processing_ && ! auto_commit_) {
            std::cout << "command was ignored. no transaction started yet" << std::endl;
            return false;
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
        bool implicit_tx = false;
        if (! tx_processing_) {
            if (! begin_tx(true)) {
                std::cout << "auto commit begin failed." << std::endl;
                return false;
            }
            implicit_tx = true;
        }
        call_result res{};
        jogasaki::common_cli::query_info info{};
        if (query) {
            reset_write_buffer();
            auto on_data = [&](std::string_view data) {
                if (debug_) {
                    std::cout << "write: " << jogasaki::utils::binary_printer{data.data(), data.size()} << std::endl;
                }
                std::unique_lock lk{write_buffer_mutex_};
                write_buffer_.write(data.data(), data.size());
                write_buffer_.flush();
            };
            res = sql.empty() ?
                client_.execute_prepared_query(tx_handle_, stmt_handles_[idx].first, parameters, on_data, &info) :
                client_.execute_query(tx_handle_, sql, on_data, &info);
        } else {
            res = sql.empty() ?
                client_.execute_prepared_statement(tx_handle_, stmt_handles_[idx].first, parameters) :
                client_.execute_statement(tx_handle_, sql);
        }
        if (! res) {
            print_error(res);
            if (implicit_tx) {
                // the transaction is already aborted by the error - rollback just to release it
                (void) client_.rollback(tx_handle_);
                tx_processing_ = false;
                tx_handle_ = {};
            }
            return false;
        }
        if (query) {
            show_query_result(info);
        }
        std::cout << "command completed successfully." << std::endl;
        if (implicit_tx) {
            if (! commit_tx(true)) {
                std::cout << "auto commit failed to commit tx" << std::endl;
                return false;
            }
        }
        return true;
    }
    bool issue_statement(std::vector<std::string_view>& args) {
        return issue_common(false, args);
    }
    bool issue_query(std::vector<std::string_view>& args) {
        return issue_common(true, args);
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
        std::string explained{};
        if (auto res = client_.explain(stmt_handles_[idx].first, parameters, explained); ! res) {
            print_error(res);
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
    if (! e.fill_from_flags(*cfg)) {
        return -1;
    }
    try {
        return e.run(cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}
