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

#include <jogasaki/api/transaction.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/prepared_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/scheduler/statement_scheduler.h>

#include <jogasaki/kvs/transaction.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;

class database;

/**
 * @brief transaction
 */
class transaction : public api::transaction {
public:
    transaction() = default;
    transaction(impl::database& database, bool readonly);

    status commit() override;
    status abort() override;
    status execute(api::executable_statement& statement) override;
    status execute(
        api::executable_statement& statement,
        std::unique_ptr<api::result_set>& result
    ) override;
    impl::database& database();

    status execute(
        api::prepared_statement const& prepared,
        api::parameter_set const& parameters
    ) override;

    status execute(
        api::prepared_statement const& prepared,
        api::parameter_set const& parameters,
        std::unique_ptr<api::result_set>& result
    ) override;

    bool execute_async(maybe_shared_ptr<api::executable_statement> const& statement, callback on_completion) override;
    bool execute_async(
        maybe_shared_ptr<api::executable_statement> const& statement,
        maybe_shared_ptr<data_channel> const& channel,
        callback on_completion
    ) override;

private:
    impl::database* database_{};
    scheduler::statement_scheduler scheduler_{};
    std::shared_ptr<kvs::transaction> tx_{};
    std::shared_ptr<request_context> request_context_{};
    utils::latch async_execution_latch_{true};  // latch is closed during async execution

    bool execute_async_common(
        maybe_shared_ptr<api::executable_statement> const& statement,
        maybe_shared_ptr<api::data_channel> const& channel,
        callback on_completion
    );
    void check_async_execution();
};

}
