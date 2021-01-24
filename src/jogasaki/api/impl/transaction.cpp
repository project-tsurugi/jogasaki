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
#include "transaction.h"
#include "executable_statement.h"

#include <takatori/util/downcast.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;

status transaction::commit() {
    return tx_->commit();
}

status transaction::abort() {
    return tx_->abort();
}

status transaction::execute(api::executable_statement& statement) {
    std::unique_ptr<api::result_set> result{};
    return execute(statement, result);
}

status transaction::execute(api::executable_statement& statement, std::unique_ptr<api::result_set>& result) {
    auto& s = unsafe_downcast<impl::executable_statement&>(statement);
    auto& e = s.body();
    auto store = std::make_unique<data::result_store>();
    auto request_ctx = std::make_shared<request_context>(
        std::make_shared<class channel>(),
        database_->configuration(),
        s.resource(),
        database_->kvs_db(),
        tx_,
        store.get()
    );
    if (e->is_execute()) {
        auto* stmt = unsafe_downcast<executor::common::execute>(e->operators());
        auto& g = stmt->operators();
        g.context(*request_ctx);
        scheduler_.schedule(*stmt, *request_ctx);
        // for now, assume only one result is returned
        result = std::make_unique<impl::result_set>(
            std::move(store)
        );
        return request_ctx->status_code();
    }
    auto* stmt = unsafe_downcast<executor::common::write>(e->operators());
    scheduler_.schedule(*stmt, *request_ctx);
    return request_ctx->status_code();
}

impl::database& transaction::database() {
    return *database_;
}

transaction::transaction(
    impl::database& database,
    bool readonly
) :
    database_(std::addressof(database)),
    scheduler_(database_->configuration()),
    tx_(database_->kvs_db()->create_transaction(readonly))
{}

}
