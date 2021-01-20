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
#include "database.h"

#include <jogasaki/api/impl/result_set.h>

#include <string_view>
#include <memory>

namespace jogasaki::api::impl {

bool database::execute(std::string_view sql, std::unique_ptr<api::result_set>& result) {
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    if(! plan::compile(sql, *ctx, {})) {
        LOG(ERROR) << "compilation failed.";
        return false;
    }
    if (! kvs_db_) {
        LOG(ERROR) << "database not started";
        return false;
    }
    auto store = std::make_unique<data::result_store>();
    // TODO redesign how request context is passed
    auto e = ctx->executable_statement();
    auto request_ctx = std::make_shared<request_context>(
        std::make_shared<class channel>(),
        cfg_,
        std::move(resource),
        kvs_db_,
        kvs_db_->create_transaction(),  // TODO retrieve from api transaction object
        store.get()
    );
    if (e->is_execute()) {
        auto* stmt = unsafe_downcast<executor::common::execute>(e->operators());
        auto& g = stmt->operators();
        g.context(*request_ctx);
        scheduler_.schedule(*stmt, *request_ctx);
        if (store->size() > 0) {
            // for now, assume only one result is returned
            result = std::make_unique<impl::result_set>(
                std::move(store)
            );
        }
        return true;
    }
    auto* stmt = unsafe_downcast<executor::common::write>(e->operators());
    scheduler_.schedule(*stmt, *request_ctx);
    return true;
}

bool database::execute(std::string_view sql) {
    std::unique_ptr<api::result_set> result{};
    return execute(sql, result);
}

database* database::get_impl(api::database& arg) noexcept {
    return unsafe_downcast<database>(std::addressof(arg));
}

std::shared_ptr<kvs::database> const& database::kvs_db() const noexcept {
    return kvs_db_;
}

std::shared_ptr<yugawara::storage::configurable_provider> const& database::tables() const noexcept {
    return tables_;
}

std::shared_ptr<yugawara::aggregate::configurable_provider> const& database::aggregate_functions() const noexcept {
    return aggregate_functions_;
}

bool database::start() {
    if (! kvs_db_) {
        kvs_db_ = kvs::database::open();
    }
    if (! kvs_db_) {
        LOG(ERROR) << "opening db failed";
        return false;
    }
    bool success = true;
    tables_->each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const&) {
        success = success && kvs_db_->create_storage(id);
    });
    if (! success) {
        LOG(ERROR) << "creating table schema entries failed";
    }
    return success;
}

bool database::stop() {
    if (kvs_db_) {
        if(!kvs_db_->close()) {
            return false;
        }
        kvs_db_ = nullptr;
    }

    aggregate_functions_.reset();
    tables_.reset();
    return true;
}

database::database(std::shared_ptr<configuration> cfg) :
    cfg_(std::move(cfg)),
    scheduler_(cfg_)
{
    executor::add_builtin_tables(*tables_);
    executor::function::add_builtin_aggregate_functions(*aggregate_functions_, global::function_repository());
}

}

namespace jogasaki::api {

std::unique_ptr<database> create_database(std::shared_ptr<configuration> cfg) {
    return std::make_unique<impl::database>(std::move(cfg));
}

}
