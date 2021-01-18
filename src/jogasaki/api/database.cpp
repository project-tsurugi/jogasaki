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
#include "database_impl.h"

#include <string_view>
#include <glog/logging.h>

#include <takatori/util/fail.h>
#include <takatori/util/downcast.h>

#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_impl.h>
#include <jogasaki/request_context.h>
#include <jogasaki/channel.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>

namespace jogasaki::api {

using configurable_provider = ::yugawara::storage::configurable_provider;
using takatori::util::fail;
using takatori::util::unsafe_downcast;

bool database::impl::execute(std::string_view sql, std::unique_ptr<result_set>& result) {
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
        // for now, assume only one result is returned
        result = std::make_unique<result_set>(std::make_unique<result_set::impl>(
            std::move(store)
        ));
        return true;
    }
    auto* stmt = unsafe_downcast<executor::common::write>(e->operators());
    scheduler_.schedule(*stmt, *request_ctx);
    return true;
}

bool database::impl::start() {
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

bool database::impl::stop() {
    if (kvs_db_) {
        if(!kvs_db_->close()) {
            return false;
        }
        kvs_db_ = nullptr;
    }
    return true;
}
database::database() : impl_(std::make_unique<database::impl>()) {}
database::~database() = default;

bool database::execute(std::string_view sql, std::unique_ptr<result_set>& result) {
    return impl_->execute(sql, result);
}

bool database::execute(std::string_view sql) {
    std::unique_ptr<result_set> result{};
    return execute(sql, result);
}

bool database::start() {
    return impl_->start();
}

bool database::stop() {
    return impl_->stop();
}

}
