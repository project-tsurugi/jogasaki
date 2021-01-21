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

#include <jogasaki/api/transaction.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/scheduler/statement_scheduler.h>

#include <jogasaki/kvs/transaction.h>

namespace jogasaki::api::impl {

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
    status execute(api::executable_statement& statement, std::unique_ptr<api::result_set>& result) override;
    impl::database& database();
private:
    impl::database* database_{};
    scheduler::statement_scheduler scheduler_{};
    std::shared_ptr<kvs::transaction> tx_{};
};

}
