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
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <future>
#include <iostream>
#include <linenoise.h>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>
#include <boost/filesystem/path.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <jogasaki/configuration.h>

#include "input_parser.h"
#include "output.h"
#include "service_client.h"
#include "service_setup.h"
#include "session.h"
#include "statement.h"
#include "text_utils.h"

DEFINE_string(input_file, "", "script file to execute. If not specified, read from stdin (as script if stdin is not a terminal)");  //NOLINT
DEFINE_string(transaction, "short", "type of transactions started implicitly: short (OCC), long (LTX), read (RTX), or manual (never started implicitly)");  //NOLINT
DEFINE_string(write_preserve, "", "comma separated table names to write preserve for transactions started implicitly with --transaction=long");  //NOLINT
DEFINE_bool(auto_commit, false, "commit after each statement even in explicitly started transaction");  //NOLINT
DEFINE_bool(stop_on_error, false, "stop processing the script on error");  //NOLINT
DEFINE_int64(wait_timeout, 60000, "timeout (ms) waiting for a session to complete in script mode. Specify 0 to wait forever");  //NOLINT
DEFINE_bool(timestamp, false, "print timestamp for each output line");  //NOLINT
DEFINE_bool(display_implicit, true, "print messages for transactions started or ended implicitly (same as tgsql client variable display.implicit)");  //NOLINT
DEFINE_bool(raw, false, "print raw result set data");  //NOLINT
DEFINE_bool(test_build, false, "To verify build of this executable");  //NOLINT
DEFINE_string(history_file, "", "specify the command history file name. Defaults to $HOME/.service_cli_history");  //NOLINT
DEFINE_int64(prompt_wait, 1000, "max duration (ms) to wait for the sessions that received the input before showing the next prompt in interactive mode. Results that arrive later are printed asynchronously. Specify 0 to wait forever");  //NOLINT
DEFINE_int32(exit_on_idle, -1, "Exit the program if user leaves the command line idle. Specify the duration in second, or -1 not to exit.");  //NOLINT

namespace tateyama::service_cli {

using namespace std::chrono_literals;
using jogasaki::common_cli::begin_options;

constexpr std::string_view initial_session_name = "T0";

std::string default_history_file() {
    if (auto* home = std::getenv("HOME"); home != nullptr) {
        return (boost::filesystem::path{home} / ".service_cli_history").string();
    }
    return ".service_cli_history";
}

void print_help(output& out) {
    out.print("", std::vector<std::string>{
        "statements (terminated by ';'):",
        "  <sql>                                execute sql statement",
        "  BEGIN [LONG] TRANSACTION [options]   start transaction (options: READ ONLY [DEFERRABLE|IMMEDIATE], READ WRITE,",
        "                                       WRITE PRESERVE <tables>, READ AREA INCLUDE <tables> EXCLUDE <tables>,",
        "                                       INCLUDE DDL, EXECUTE PRIOR|EXCLUDING [DEFERRABLE|IMMEDIATE], AS <label>)",
        "  COMMIT [WAIT FOR ACCEPTED|AVAILABLE|STORED|PROPAGATED]",
        "  ROLLBACK",
        "  EXPLAIN <sql>                        show execution plan (json)",
        "  \\prepare <name> [<var>:<type> ...] AS <sql>   prepare statement with placeholders",
        "  \\execute <name> [<var>=<value> ...]           execute prepared statement",
        "special commands (terminated by line end):",
        "  \\show table [<table name>]          list tables or describe the table",
        "  \\status                             show sessions status",
        "  \\timing [on|off]                    show/hide execution time",
        "  \\help                               show this help",
        "  \\exit                               rollback transactions and exit",
        "  \\halt                               exit immediately",
        "sessions (tgsqlmux compatible):",
        "  <name>: <statement>                  execute the statement on the session",
        "  <name>:                              switch the target session",
        "directives (comments for tgsql):",
        "  --\\sleep <seconds>                  sleep",
        "  --\\wait [<name>]                    wait for the session (or all sessions) to complete",
        "  --\\expect_error                     expect the next statement on the target session fails",
    });
}

class cli {
public:
    bool init() {
        if (FLAGS_transaction == "short" || FLAGS_transaction == "occ") {
            settings_.implicit_options_.kind_ = begin_options::kind::occ;
        } else if (FLAGS_transaction == "long" || FLAGS_transaction == "ltx") {
            settings_.implicit_options_.kind_ = begin_options::kind::ltx;
        } else if (FLAGS_transaction == "read" || FLAGS_transaction == "readonly" || FLAGS_transaction == "rtx") {
            settings_.implicit_options_.kind_ = begin_options::kind::rtx;
        } else if (FLAGS_transaction == "manual") {
            settings_.manual_ = true;
        } else {
            LOG(ERROR) << "invalid --transaction: " << FLAGS_transaction;
            return false;
        }
        std::stringstream ss{FLAGS_write_preserve};
        for(std::string t{}; std::getline(ss, t, ',');) {
            if (! trim(t).empty()) {
                settings_.implicit_options_.write_preserves_.emplace_back(trim(t));
            }
        }
        settings_.auto_commit_ = FLAGS_auto_commit;
        settings_.raw_ = FLAGS_raw;
        settings_.display_implicit_ = FLAGS_display_implicit;
        wait_timeout_ = std::chrono::milliseconds{FLAGS_wait_timeout};
        interactive_ = FLAGS_input_file.empty() && ::isatty(STDIN_FILENO) != 0;
        settings_.echo_ = ! interactive_;
        return true;
    }

    int run(std::shared_ptr<jogasaki::configuration> cfg) {
        if (! env_.start(std::move(cfg))) {
            return -1;
        }
        if (! jogasaki::common_cli::setup_builtin_tables(env_.database())) {
            env_.stop();
            return -1;
        }
        if (! FLAGS_test_build) {
            if (interactive_) {
                run_interactive();
            } else {
                run_script();
            }
        }
        for(auto&& [name, s] : sessions_) {
            s->close(! halt_);
        }
        sessions_.clear();
        env_.stop();
        auto errors = settings_.errors_.load();
        if (errors > 0) {
            out_.print("", std::to_string(errors) + " error(s) occurred");
            return 1;
        }
        return 0;
    }

private:
    jogasaki::common_cli::service_environment env_{};
    session_settings settings_{};
    output out_{FLAGS_timestamp};
    input_parser parser_{std::string{initial_session_name}};
    std::map<std::string, std::unique_ptr<session>> sessions_{};
    std::size_t next_session_id_{1};
    std::chrono::milliseconds wait_timeout_{};
    std::set<std::string> submitted_{};
    bool interactive_{};
    bool exit_{};
    bool halt_{};

    session& get_session(std::string const& name) {
        auto it = sessions_.find(name);
        if (it != sessions_.end()) {
            return *it->second;
        }
        auto s = std::make_unique<session>(name, next_session_id_++, env_.service(), settings_, out_);
        return *sessions_.emplace(name, std::move(s)).first->second;
    }

    void print_status() {
        std::vector<std::string> lines{};
        for(auto&& [name, s] : sessions_) {
            lines.emplace_back(name + ": " + s->status());
        }
        if (lines.empty()) {
            lines.emplace_back("no session");
        }
        out_.print("", lines);
    }

    // wait for the session to become idle. Exits the process on timeout to avoid hanging forever.
    void wait_session(session& s) {
        if (s.wait_idle(wait_timeout_)) {
            return;
        }
        out_.print("", "error: timed out waiting for session " + s.name() + " (--wait_timeout=" +
            std::to_string(wait_timeout_.count()) + "ms). sessions status:");
        print_status();
        std::cout << std::flush;
        std::_Exit(2);
    }

    void wait_all() {
        for(auto&& [name, s] : sessions_) {
            wait_session(*s);
        }
    }

    // handle the statements processed by the main thread. Returns false if the statement should go to the session.
    bool handle_main_command(input_item const& item) {
        auto trimmed = trim(item.text_);
        if (! starts_with(trimmed, "\\")) {
            return false;
        }
        auto name = to_lower(first_word(trimmed.substr(1)));
        auto arg = to_lower(trim(trimmed.substr(1 + name.size())));
        if (name != "status" && ! interactive_ && sessions_.count(item.session_) != 0) {
            // keep the order of outputs in script mode. \status doesn't wait so that it can show running statements
            wait_session(*sessions_.at(item.session_));
        }
        if (name == "exit" || name == "quit") {
            exit_ = true;
            return true;
        }
        if (name == "halt") {
            exit_ = true;
            halt_ = true;
            return true;
        }
        if (name == "status") {
            print_status();
            return true;
        }
        if (name == "help" || name == "h" || name == "?") {
            print_help(out_);
            return true;
        }
        if (name == "timing") {
            if (arg == "on") {
                settings_.timing_ = true;
            } else if (arg == "off") {
                settings_.timing_ = false;
            } else {
                settings_.timing_ = ! settings_.timing_;
            }
            out_.print("", std::string{"Timing is "} + (settings_.timing_ ? "on." : "off."));
            return true;
        }
        if (name == "connect" || name == "disconnect") {
            out_.print("", "\\" + name + " is ignored since the database runs in-process");
            return true;
        }
        return false;
    }

    void process(std::vector<input_item> const& items) {
        for(auto&& item : items) {
            if (exit_) {
                return;
            }
            switch(item.kind_) {
                case input_item::kind::statement: {
                    if (handle_main_command(item)) {
                        break;
                    }
                    auto& s = get_session(item.session_);
                    if (! interactive_) {
                        // same as tgsqlmux, wait for the target session before sending next one
                        wait_session(s);
                    }
                    s.submit_statement(item.text_);
                    if (interactive_) {
                        submitted_.emplace(item.session_);
                    }
                    break;
                }
                case input_item::kind::sleep:
                    std::this_thread::sleep_for(std::chrono::duration<double>(item.seconds_));
                    break;
                case input_item::kind::wait: {
                    auto target = std::string{trim(item.text_)};
                    if (target.empty()) {
                        wait_all();
                    } else if (auto it = sessions_.find(target); it != sessions_.end()) {
                        wait_session(*it->second);
                    }
                    break;
                }
                case input_item::kind::expect_error: {
                    auto& s = get_session(item.session_);
                    if (! interactive_) {
                        wait_session(s);
                    }
                    s.submit_expect_error();
                    if (interactive_) {
                        submitted_.emplace(item.session_);
                    }
                    break;
                }
                case input_item::kind::unknown_directive:
                    if (! interactive_ && sessions_.count(item.session_) != 0) {
                        wait_session(*sessions_.at(item.session_));
                    }
                    out_.print("", "warning: unknown directive is ignored (line " + std::to_string(item.line_) + "): --\\" + item.text_);
                    break;
            }
            if (! interactive_ && FLAGS_stop_on_error) {
                auto& s = get_session(item.session_);
                wait_session(s);
                if (settings_.errors_ > 0) {
                    out_.print("", "stop processing the script on error (--stop_on_error)");
                    exit_ = true;
                    return;
                }
            }
        }
    }

    void run_script() {
        std::ifstream file{};
        std::istream* in = &std::cin;
        if (! FLAGS_input_file.empty()) {
            file.open(FLAGS_input_file);
            if (! file) {
                out_.print("", "error: failed to open input file: " + FLAGS_input_file);
                ++settings_.errors_;
                return;
            }
            in = &file;
        }
        for(std::string line{}; ! exit_ && std::getline(*in, line);) {
            process(parser_.feed(line));
        }
        if (! exit_) {
            process(parser_.finish());
        }
        if (! halt_) {
            wait_all();
        }
    }

    // wait for the sessions that received the input so that the prompt follows the results.
    // Gives up after --prompt_wait so that the console is available even if the session is blocked (e.g. waiting for
    // other transactions)
    void wait_submitted() {
        using clock = std::chrono::steady_clock;
        auto deadline = clock::now() + std::chrono::milliseconds{FLAGS_prompt_wait};
        for(auto&& name : submitted_) {
            auto it = sessions_.find(name);
            if (it == sessions_.end()) {
                continue;
            }
            if (FLAGS_prompt_wait == 0) {
                it->second->wait_idle(std::chrono::milliseconds{0});
                continue;
            }
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - clock::now());
            if (remaining.count() <= 0 || ! it->second->wait_idle(remaining)) {
                break;
            }
        }
        submitted_.clear();
    }

    void run_interactive() {
        using clock = std::chrono::steady_clock;
        std::atomic<clock::time_point> last_interacted{clock::now()};
        std::atomic_bool done{false};
        auto idle_watcher = std::async(std::launch::async, [&]() {
            if (FLAGS_exit_on_idle <= 0) {
                return;
            }
            while(! done) {
                std::this_thread::sleep_for(10ms);
                if (clock::now() - last_interacted.load() > std::chrono::seconds{FLAGS_exit_on_idle}) {
                    std::cerr << std::endl << "Program exits because no interaction has been made for " << FLAGS_exit_on_idle << " secs." << std::endl;
                    std::_Exit(1);
                }
            }
        });
        auto history = FLAGS_history_file.empty() ? default_history_file() : FLAGS_history_file;
        linenoiseHistoryLoad(history.c_str());
        out_.print("", "type \\help to show commands");
        while(! exit_) {
            auto& current = parser_.current_session();
            std::string prompt = parser_.has_partial(current) ? "     | " : current + "> ";
            last_interacted = clock::now();
            auto* l = linenoise(prompt.c_str());
            last_interacted = clock::now();
            if (l == nullptr) {
                // EOF (Ctrl-D)
                break;
            }
            std::string line{l};
            free(l);  //NOLINT
            if (! trim(line).empty()) {
                linenoiseHistoryAdd(line.c_str());
                linenoiseHistorySave(history.c_str());
            }
            process(parser_.feed(line));
            wait_submitted();
        }
        if (! halt_) {
            wait_all();
        }
        done = true;
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
    gflags::SetUsageMessage(
        "service cli - execute sql on in-process database via sql service api. "
        "Accepts tgsql scripts and tgsqlmux session prefixes. Type \\help on console for commands.");
    // suppress engine info logs mixed into the console output by default
    jogasaki::common_cli::set_flag_defaults({{"minloglevel", "1"}});
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_test_build) {
        jogasaki::common_cli::set_flag_defaults({
            {"thread_count", "1"},
            {"partitions", "1"},
        });
    }
    auto cfg = std::make_shared<jogasaki::configuration>();
    if (! jogasaki::common_cli::fill_configuration(*cfg)) {
        return -1;
    }
    tateyama::service_cli::cli c{};
    if (! c.init()) {
        return -1;
    }
    try {
        return c.run(cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}
