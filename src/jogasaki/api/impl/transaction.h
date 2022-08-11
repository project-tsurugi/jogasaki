/*
 * Copyright 2018-2020 tsurugi project.
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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/utils/latch.h>

#include <jogasaki/transaction_context.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;

class database;

/**
 * @brief transaction
 */
class transaction {
public:
    using callback = transaction_handle::callback;

    transaction() = default;
    transaction(impl::database& database,
        kvs::transaction_option const& options
    );

    status commit();
    status abort();
    impl::database& database();

    status execute(
        api::executable_statement& statement,
        std::unique_ptr<api::result_set>& result
    );

    status execute(
        api::statement_handle prepared,
        std::shared_ptr<api::parameter_set> parameters,
        std::unique_ptr<api::result_set>& result
    );

    bool execute_async(
        maybe_shared_ptr<api::executable_statement> const& statement,
        maybe_shared_ptr<data_channel> const& channel,
        callback on_completion
    );

    bool execute_async(
        api::statement_handle prepared,
        std::shared_ptr<api::parameter_set> parameters,
        maybe_shared_ptr<executor::io::record_channel> const& channel,
        callback on_completion,
        bool sync = false
    );

    bool execute_context(
        std::shared_ptr<request_context> rctx,
        maybe_shared_ptr<api::executable_statement> const& statement,
        maybe_shared_ptr<executor::io::record_channel> const& channel,
        callback on_completion, //NOLINT(performance-unnecessary-value-param)
        bool sync
    );

    constexpr static std::size_t undefined = static_cast<std::size_t>(-1);
    bool execute_dump(
        maybe_shared_ptr<api::executable_statement> const& statement,
        maybe_shared_ptr<api::data_channel> const& channel,
        std::string_view directory,
        callback on_completion,
        std::size_t max_records_per_file = undefined,
        bool keep_files_on_error = false
    );

    bool execute_load(
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        std::vector<std::string> files,
        callback on_completion
    );

    std::shared_ptr<request_context> create_request_context(
        maybe_shared_ptr<executor::io::record_channel> const& channel,
        std::shared_ptr<memory::lifo_paged_memory_resource> resource
    );
private:
    impl::database* database_{};
    std::shared_ptr<transaction_context> tx_{};

    bool execute_internal(
        maybe_shared_ptr<api::executable_statement> const& statement,
        maybe_shared_ptr<executor::io::record_channel> const& channel,
        callback on_completion,  //NOLINT(performance-unnecessary-value-param)
        bool sync
    );

};

}
