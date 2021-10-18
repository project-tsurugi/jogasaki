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
#include <linenoise.h>
#include <takatori/util/fail.h>
#include <takatori/util/downcast.h>

#include <tateyama/api/environment.h>
#include <tateyama/api/server/service.h>
#include <tateyama/api/endpoint/service.h>
#include <tateyama/api/registry.h>

#include <tateyama/api/endpoint/mock/endpoint_impls.h>

#include <jogasaki/api.h>
#include <jogasaki/utils/mock/command_utils.h>
#include <jogasaki/utils/mock/msgbuf_utils.h>
#include <jogasaki/utils/binary_printer.h>

DEFINE_bool(single_thread, false, "Whether to run on serial scheduler");  //NOLINT
DEFINE_bool(work_sharing, false, "Whether to use on work sharing scheduler when run parallel");  //NOLINT
DEFINE_int32(thread_count, 1, "Number of threads");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(explain, false, "explain the execution plan");  //NOLINT
DEFINE_int32(partitions, 10, "Number of partitions per process");  //NOLINT
DEFINE_bool(steal, false, "Enable stealing for task scheduling");  //NOLINT
DEFINE_bool(auto_commit, true, "Whether to commit when finishing each statement.");  //NOLINT
DEFINE_bool(prepare_data, false, "Whether to prepare a few records in the storages");  //NOLINT
DEFINE_bool(verify_record, false, "Whether to deserialize the query result records");  //NOLINT
DEFINE_bool(test_build, false, "To verify build of this executable");  //NOLINT

namespace tateyama::service_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;
using tateyama::api::endpoint::response_code;

class cli {
    std::shared_ptr<jogasaki::api::database> db_{};
public:

    int run(std::shared_ptr<jogasaki::configuration> cfg) {
        db_ = jogasaki::api::create_database(cfg);
        db_->start();
        debug_ = FLAGS_debug;
        verify_query_records_ = FLAGS_verify_record;

//        add_benchmark_tables(*impl->tables());
//        register_kvs_storage(*impl->kvs_db(), *impl->tables());

        environment_ = std::make_unique<tateyama::api::environment>();
        auto app = tateyama::api::registry<tateyama::api::server::service>::create("jogasaki");
        environment_->add_application(app);
        app->initialize(*environment_, db_.get());
        service_ = tateyama::api::endpoint::create_service(*environment_);
        environment_->endpoint_service(service_);
        auto endpoint = tateyama::api::registry<tateyama::api::endpoint::provider>::create("mock");
        environment_->add_endpoint(endpoint);
        endpoint->initialize(*environment_, {});

        bool run = true;
        if (fLB::FLAGS_test_build) {
            run = false;
        }
        while(run) {
            auto* l = linenoise("> ");
            if (! l) continue;
            linenoiseHistoryAdd(l);
            std::string input{l};
            free(l);  //NOLINT
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
                    run = false;
                    break;
                case '#':  // comment line
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
            "";
    }
    bool begin_tx() {
        if (tx_processing_) {
            std::cout << "command was ignored. transaction already started: " << tx_handle_ << std::endl;
            return false;
        }
        auto s = jogasaki::api::encode_begin(false);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto st = (*service_)(req, res);
        bool error{false};
        if(st != tateyama::status::ok || !res->completed() || res->code_ != response_code::success) {
            std::cerr << "error executing command" << std::endl;
            error = true;
        }
        tx_handle_ = jogasaki::api::decode_begin(res->body_);
        if(error) return false;
        std::cout << "transaction begin: " << tx_handle_ << std::endl;
        tx_processing_ = true;
        return true;
    }
    bool handle_result_only(std::string_view body) {
        auto [success, error] = jogasaki::api::decode_result_only(body);
        if (success) {
            std::cout << "command completed successfully." << std::endl;
            return true;
        }
        std::cerr << "command returned error: " << error.status_ << " " << error.message_ << std::endl;
        return false;
    }

    // block wait for async query execution
    void wait_for_statements() {
        for(auto&& f : on_going_statements_) {
            (void)f.get();
        }
        on_going_statements_.clear();
    }
    bool commit_tx() {
        if (! tx_processing_) {
            std::cout << "command was ignored. no transaction started yet" << std::endl;
            return false;
        }
        wait_for_statements(); // commit destoroys tx and related graphs
        auto s = jogasaki::api::encode_commit(tx_handle_);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto st = (*service_)(req, res);
        if(st != tateyama::status::ok || !res->completed() || res->code_ != response_code::success) {
            std::cerr << "error executing command" << std::endl;
        }
        auto ret = handle_result_only(res->body_);
        if (ret) {
            tx_processing_ = false;
            tx_handle_ = -1;
        }
        return ret;
    }
    bool abort_tx() {
        if (! tx_processing_) {
            std::cout << "command was ignored. no transaction started yet" << std::endl;
            return false;
        }
        wait_for_statements();
        auto s = jogasaki::api::encode_rollback(tx_handle_);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto st = (*service_)(req, res);
        if(st != tateyama::status::ok || !res->completed() || res->code_ != response_code::success) {
            std::cerr << "error executing command" << std::endl;
        }
        auto ret = handle_result_only(res->body_);
        if (ret) {
            tx_processing_ = false;
            tx_handle_ = -1;
        }
        return ret;
    }
    bool prepare(std::vector<std::string_view> const& args) {
        if (args.size() > 1) {
            std::cout << "command was ignored. too many command args" << std::endl;
            return false;
        }
        std::string sql{args[0]};
        auto s = jogasaki::api::encode_prepare(sql);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto st = (*service_)(req, res);
        bool error{false};
        if(st != tateyama::status::ok || !res->completed() || res->code_ != response_code::success) {
            std::cerr << "error executing command" << std::endl;
            error = true;
        }
        auto stmt_handle = jogasaki::api::decode_prepare(res->body_);
        if (error) return false;
        std::cout << "statement prepared: " << stmt_handle << std::endl;
        stmt_handles_.emplace_back(stmt_handle, sql);
        return true;
    }
    bool list_statements() {
        std::size_t i{};
        if (stmt_handles_.empty()) {
            std::cout << "no entry" << std::endl;
            return true;
        }
        for(auto [handle, sql] : stmt_handles_) {
            std::cout << "#" << i << " " << handle << " : " << sql << std::endl;
            ++i;
        }
        return true;
    }
    bool issue_common(bool query, std::vector<std::string_view> const& args, std::function<void(std::string_view)> on_write) {
        if (args.empty() || args.size() > 1) {
            std::cout << "command was ignored. missing or too many command args" << std::endl;
            return false;
        }
        if (! tx_processing_) {
            std::cout << "command was ignored. no transaction started yet" << std::endl;
            return false;
        }
        std::string arg{args[0]};
        std::int32_t idx{};
        bool sql_string{false};
        try {
            idx = std::stol(arg);
        } catch(std::exception& e) {
            sql_string = true;
        }
        std::vector<jogasaki::api::parameter> parameters{};
        auto s = query ? (
            sql_string ?
                jogasaki::api::encode_execute_query(tx_handle_, arg) :
                jogasaki::api::encode_execute_prepared_query(tx_handle_, stmt_handles_[idx].first, parameters)
        ) : (
            sql_string ?
                jogasaki::api::encode_execute_statement(tx_handle_, arg) :
                jogasaki::api::encode_execute_prepared_statement(tx_handle_, stmt_handles_[idx].first, parameters)
        );
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        if (query) {
            res->set_on_write(std::move(on_write));
        }
        on_going_statements_.emplace_back(
            std::async(std::launch::async, [&, res]() {
                while(! res->completed()) {
                    std::this_thread::sleep_for(20ms);
                }
                if(res->code_ != response_code::success) {
                    std::cerr << "error executing command" << std::endl;
                }
                if (verify_query_records_) {
                    auto recs = jogasaki::api::deserialize_msg(write_buffer_.str(), query_meta_);
                    for(auto&& r : recs) {
                        std::cout << "record : " << r << std::endl;
                    }
                }
                return handle_result_only(res->body_);
            })
        );
        auto st = (*service_)(req, res);
        if(st != tateyama::status::ok) {
            std::cerr << "service invocation failed" << std::endl;
            return false;
        }
        if (query) {
            auto [name, columns] = jogasaki::api::decode_execute_query(res->body_head_);
            std::cout << "query name : " << name << std::endl;
            query_meta_ = jogasaki::api::create_record_meta(std::move(columns));
            std::size_t ind{};
            for(auto&& f : query_meta_) {
                std::cout << "column " << idx << " type " << f << std::endl;
                ++ind;
            }
            write_buffer_.seekp(0);
            write_buffer_.clear();
        }
        return true;
    }
    bool issue_statement(std::vector<std::string_view> const& args) {
        return issue_common(false, args, [](std::string_view){});
    }
    bool issue_query(std::vector<std::string_view> const& args) {
        return issue_common(true, args, [&](std::string_view data) {
            std::cout << "write: " << jogasaki::utils::binary_printer{data.data(), data.size()} << std::endl;
            if (verify_query_records_) {
                std::unique_lock lk{write_buffer_mutex_};
                write_buffer_.write(data.data(), data.size());
                write_buffer_.flush();
            }
        });
    }

    std::shared_ptr<tateyama::api::endpoint::service> service_{};  //NOLINT
    std::unique_ptr<tateyama::api::environment> environment_{};  //NOLINT
    std::uint64_t tx_handle_{};  //NOLINT
    bool tx_processing_{};  //NOLINT
    bool debug_{};    //NOLINT
    bool verify_query_records_{};
    std::mutex write_buffer_mutex_{};
    std::stringstream write_buffer_{};
    std::vector<std::pair<std::uint64_t, std::string>> stmt_handles_{};
    std::vector<std::future<bool>> on_going_statements_{};
    jogasaki::meta::record_meta query_meta_{};
};

}  // namespace

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
    try {
        e.run(cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
