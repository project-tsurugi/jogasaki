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
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/compiler.h>

#include <string_view>
#include <memory>
#include <takatori/serializer/json_printer.h>


namespace jogasaki::api::impl {

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

    // destorying providers in destructor cause pure virtual function call, so reset here // FIXME
    aggregate_functions_.reset();
    tables_.reset();

    return true;
}

database::database(std::shared_ptr<class configuration> cfg) :
    cfg_(std::move(cfg))
{
    executor::add_builtin_tables(*tables_);
    executor::function::add_builtin_aggregate_functions(*aggregate_functions_, global::function_repository());
    if(cfg_->prepare_benchmark_tables()) {
        executor::add_benchmark_tables(*tables_);
    }
}

std::shared_ptr<class configuration> const& database::configuration() const noexcept {
    return cfg_;
}

database::database() : database(std::make_shared<class configuration>()) {}

bool database::prepare(std::string_view sql, std::unique_ptr<api::prepared_statement>& statement) {
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    if(! plan::prepare(sql, *ctx)) {
        LOG(ERROR) << "compilation failed.";
        return false;
    }
    statement = std::make_unique<impl::prepared_statement>(ctx->prepared_statement());
    return true;
}

bool database::create_executable(std::string_view sql, std::unique_ptr<api::executable_statement>& statement) {
    std::unique_ptr<api::prepared_statement> prepared{};
    if(auto rc = prepare(sql, prepared); !rc) {
        return false;
    }
    std::unique_ptr<api::executable_statement> exec{};
    impl::parameter_set parameters{};
    if(auto rc = resolve(*prepared, parameters, exec); !rc) {
        return false;
    }
    statement = std::make_unique<impl::executable_statement>(
        unsafe_downcast<impl::executable_statement>(*exec).body(),
        unsafe_downcast<impl::executable_statement>(*exec).resource()
    );
    return true;
}

std::unique_ptr<api::transaction> database::do_create_transaction(bool readonly) {
    if (! kvs_db_) {
        LOG(ERROR) << "database not started";
        return {};
    }
    return std::make_unique<impl::transaction>(*this, readonly);
}

bool database::resolve(
    api::prepared_statement const& prepared,
    api::parameter_set const& parameters,
    std::unique_ptr<api::executable_statement>& statement
) {
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    ctx->prepared_statement(
        unsafe_downcast<impl::prepared_statement>(prepared).body()
    );
    auto params = unsafe_downcast<impl::parameter_set>(parameters).body();
    if(! plan::compile(*ctx, params.get())) {
        LOG(ERROR) << "compilation failed.";
        return false;
    }
    statement = std::make_unique<impl::executable_statement>(
        ctx->executable_statement(),
        std::move(resource)
    );
    return true;
}

bool database::explain(api::executable_statement const& executable, std::ostream& out) {
    auto r = unsafe_downcast<impl::executable_statement>(executable).body();
    r->compiled_info().object_scanner()(
        r->statement(),
        takatori::serializer::json_printer{ out }
    );
    return true;
}

}

namespace jogasaki::api {

std::unique_ptr<database> create_database(std::shared_ptr<class configuration> cfg) {
    return std::make_unique<impl::database>(std::move(cfg));
}

}
