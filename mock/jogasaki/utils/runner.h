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
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>

namespace jogasaki::utils {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::executor;

using takatori::util::maybe_shared_ptr;

/**
 * @brief statement runner
 * @details builder object to collect necessary properties to execute a statement
 */
class runner {
public:
    runner() = default;
    ~runner() = default;
    runner(runner const& other) = default;
    runner& operator=(runner const& other) = default;
    runner(runner&& other) noexcept = default;
    runner& operator=(runner&& other) noexcept = default;

    /**
     * @brief db setter
     * @param arg db that runs the statement
     * @return *this
     */
    runner& db(jogasaki::api::database& arg) {
        db_ = &arg;
        return *this;
    }

    /**
     * @brief sql text setter
     * @param arg sql text to execute
     * @return *this
     */
    runner& text(std::string_view arg) {
        text_ = std::string{arg};
        return *this;
    }

    /**
     * @brief statement handle setter
     * @param arg statement to execute
     * @return *this
     */
    runner& prepared(api::statement_handle arg) {
        prepared_ = arg;
        return *this;
    }

    /**
     * @brief executable statement setter
     * @param arg executable statement to execute
     * @return *this
     */
    runner& executable(api::executable_statement& stmt) {
        executable_ = &stmt;
        return *this;
    }

    /**
     * @brief set flag to run explain
     * @param arg the output object filled with the explain result on run()
     * @return *this
     */
    runner& explain(std::string& arg) {
        explain_output_ = &arg;
        return *this;
    }

    /**
     * @brief set variable definition
     * @param arg variable definition used to compile and run the statement
     * @return *this
     */
    runner& vars(
        std::unordered_map<std::string, api::field_type_kind> const& variables
    ) {
        variables_ = &variables;
        return *this;
    }

    /**
     * @brief set parameters
     * @param arg parameter set used to run the statement
     * @return *this
     */
    runner& params(api::parameter_set const& arg) {
        params_ = &arg;
        return *this;
    }

    /**
     * @brief set transaction
     * @param arg transaction used to run the statement
     * @return *this
     */
    runner& tx(api::transaction_handle arg) {
        tx_ = arg;
        return *this;
    }

    /**
     * @brief indicate collect result set on run()
     * @param arg output object container filled on run()
     * @return *this
     */
    runner& output_records(std::vector<mock::basic_record>& out) {
        output_records_ = &out;
        return *this;
    }

    /**
     * @brief set status output object
     * @param arg output status variable filled on run()
     * @return *this
     */
    runner& st(status& out) {
        output_status_ = &out;
        return *this;
    }

    /**
     * @brief set error info output object
     * @param arg output error_info variable filled on run()
     * @return *this
     */
    runner& error(std::shared_ptr<jogasaki::error::error_info>& arg) {
        output_error_info_ = &arg;
        return *this;
    }

    /**
     * @brief set stats output object
     * @param arg output stats variable filled on run()
     * @return *this
     */
    runner& stats(std::shared_ptr<request_statistics>& arg) {
        stats_ = &arg;
        return *this;
    }

    /**
     * @brief set no_abort flag to indicate not to abort tx even if error is expected (by expect_error())
     * @param arg no_abort flag
     * @return *this
     */
    runner& no_abort(bool arg) {
        no_abort_ = arg;
        return *this;
    }

    /**
     * @brief set expect_error flag to indicate testcase expects error and tx is aborted after run (unless no_abort flag is set)
     * @param arg expect_error flag
     * @return *this
     */
    runner& expect_error(bool arg) {
        expect_error_ = arg;
        return *this;
    }

    /**
     * @brief indicate whether to dump out the result record content (effective when output_records is set)
     * @param arg flag to dump records
     * @return *this
     */
    runner& show_recs(bool arg) {
        show_recs_ = arg;
        return *this;
    }

    /**
     * @brief indicate whether to show the executed statement in textual format (explained plan)
     * @param arg flag to dump expalined plan
     * @return *this
     */
    runner& show_plan(bool arg) {
        show_plan_ = arg;
        return *this;
    }

    /**
     * @brief report the run() result
     * @return result message to report run
     */
    [[nodiscard]] std::string_view report() const noexcept {
        return execution_message_;
    }

    /**
     * @brief run the statement
     * @return result message to report run
     * @return *this
     */
    runner& run();

    /**
     * @brief return whether successful execution
     * @return whether error message is empty or not
     * @return *this
     */
    [[nodiscard]] operator bool() const noexcept {
        return execution_message_.empty();
    }

private:

    jogasaki::api::database* db_{};
    std::string text_{};
    api::statement_handle prepared_{};
    api::executable_statement* executable_{};

    std::unordered_map<std::string, api::field_type_kind> const* variables_{};  //NOLINT
    api::parameter_set const* params_{};
    api::transaction_handle tx_{};

    std::vector<mock::basic_record>* output_records_{};
    std::shared_ptr<jogasaki::error::error_info>* output_error_info_{};
    std::string* explain_output_{};
    status* output_status_{};
    std::shared_ptr<request_statistics>* stats_{};

    bool no_abort_{};
    bool show_plan_{};
    bool show_recs_{};
    bool expect_error_{};
    std::string execution_message_{};

};

}
