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

#include <takatori/util/fail.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>

#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/kvs/storage_dump.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/parallel_task_scheduler.h>
#include <jogasaki/scheduler/thread_params.h>

#include <string_view>
#include <memory>
#include <takatori/serializer/json_printer.h>

namespace jogasaki::api::impl {

using takatori::util::fail;

std::shared_ptr<kvs::database> const& database::kvs_db() const noexcept {
    return kvs_db_;
}

std::shared_ptr<yugawara::storage::configurable_provider> const& database::tables() const noexcept {
    return tables_;
}

std::shared_ptr<yugawara::aggregate::configurable_provider> const& database::aggregate_functions() const noexcept {
    return aggregate_functions_;
}

status database::start() {
    if (! kvs_db_) {
        kvs_db_ = kvs::database::open();
    }
    if (! kvs_db_) {
        LOG(ERROR) << "opening db failed";
        return status::err_io_error;
    }
    bool success = true;
    tables_->each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const&) {
        success = success && kvs_db_->create_storage(id);
    });
    if (! success) {
        LOG(ERROR) << "creating table schema entries failed";
        return status::err_io_error;
    }
    if (! task_scheduler_) {
        if (cfg_->single_thread()) {
            task_scheduler_ = std::make_unique<scheduler::serial_task_scheduler>();
        } else {
            task_scheduler_ = std::make_unique<scheduler::parallel_task_scheduler>(scheduler::thread_params(cfg_));
        }
    }
    task_scheduler_->start();
    return status::ok;
}

status database::stop() {
    task_scheduler_->stop();
    if (kvs_db_) {
        if(! kvs_db_->close()) {
            return status::err_io_error;
        }
        kvs_db_ = nullptr;
    }

    // destorying providers in destructor cause pure virtual function call, so reset here // FIXME
    aggregate_functions_.reset();
    tables_.reset();
    return status::ok;
}

database::database(
    std::shared_ptr<class configuration> cfg
) :
    cfg_(std::move(cfg))
{
    executor::add_builtin_tables(*tables_);
    executor::function::incremental::add_builtin_aggregate_functions(
        *aggregate_functions_,
        global::incremental_aggregate_function_repository()
    );
    executor::function::add_builtin_aggregate_functions(
        *aggregate_functions_,
        global::aggregate_function_repository()
    );
    if(cfg_->prepare_benchmark_tables()) {
        executor::add_benchmark_tables(*tables_);
    }

    // built-in host variables (for testing)
    host_variables_->add({"i4", takatori::type::int4{}});
    host_variables_->add({"i8", takatori::type::int8{}});
    host_variables_->add({"f4", takatori::type::float4{}});
    host_variables_->add({"f8", takatori::type::float8{}});
    host_variables_->add({"ch", takatori::type::character{takatori::type::varying}});
}

std::shared_ptr<class configuration> const& database::configuration() const noexcept {
    return cfg_;
}

database::database() : database(std::make_shared<class configuration>()) {}

status database::register_variable(std::string_view name, field_type_kind kind) {
    if (auto e = host_variables_->find(name)) {
        // ignore if it's already exists
        return status::ok;
    }
    switch(kind) {
        case field_type_kind::int4: host_variables_->add({name, takatori::type::int4{}}, true); break;
        case field_type_kind::int8: host_variables_->add({name, takatori::type::int8{}}, true); break;
        case field_type_kind::float4: host_variables_->add({name, takatori::type::float4{}}, true); break;
        case field_type_kind::float8: host_variables_->add({name, takatori::type::float8{}}, true); break;
        case field_type_kind::character: host_variables_->add({name, takatori::type::character{takatori::type::varying}}, true); break;
        default: fail();
    }
    return status::ok;
}

status database::prepare(std::string_view sql, std::unique_ptr<api::prepared_statement>& statement) {
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    ctx->variable_provider(host_variables_);
    if(auto rc = plan::prepare(sql, *ctx); rc != status::ok) {
        LOG(ERROR) << "compilation failed.";
        return rc;
    }
    statement = std::make_unique<impl::prepared_statement>(ctx->prepared_statement());
    return status::ok;
}

status database::create_executable(std::string_view sql, std::unique_ptr<api::executable_statement>& statement) {
    std::unique_ptr<api::prepared_statement> prepared{};
    if(auto rc = prepare(sql, prepared); rc != status::ok) {
        return rc;
    }
    std::unique_ptr<api::executable_statement> exec{};
    impl::parameter_set parameters{};
    if(auto rc = resolve(*prepared, parameters, exec); rc != status::ok) {
        return rc;
    }
    statement = std::make_unique<impl::executable_statement>(
        unsafe_downcast<impl::executable_statement>(*exec).body(),
        unsafe_downcast<impl::executable_statement>(*exec).resource()
    );
    return status::ok;
}

std::unique_ptr<api::transaction> database::do_create_transaction(bool readonly) {
    if (! kvs_db_) {
        LOG(ERROR) << "database not started";
        return {};
    }
    return std::make_unique<impl::transaction>(*this, readonly);
}

status database::resolve(
    api::prepared_statement const& prepared,
    api::parameter_set const& parameters,
    std::unique_ptr<api::executable_statement>& statement
) {
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    ctx->variable_provider(host_variables_);
    ctx->prepared_statement(
        unsafe_downcast<impl::prepared_statement>(prepared).body()
    );
    auto params = unsafe_downcast<impl::parameter_set>(parameters).body();
    if(auto rc = plan::compile(*ctx, params.get()); rc != status::ok) {
        LOG(ERROR) << "compilation failed.";
        return rc;
    }
    statement = std::make_unique<impl::executable_statement>(
        ctx->executable_statement(),
        std::move(resource)
    );
    return status::ok;
}

status database::explain(api::executable_statement const& executable, std::ostream& out) {
    auto r = unsafe_downcast<impl::executable_statement>(executable).body();
    r->compiled_info().object_scanner()(
        *r->statement(),
        takatori::serializer::json_printer{ out }
    );
    return status::ok;
}

void database::dump(std::ostream& output, std::string_view index_name, std::size_t batch_size) {
    kvs::storage_dump dumper{*kvs_db_};
    dumper.dump(output, index_name, batch_size);
}

void database::load(std::istream& input, std::string_view index_name, std::size_t batch_size) {
    kvs::storage_dump dumper{*kvs_db_};
    dumper.load(input, index_name, batch_size);
}

scheduler::task_scheduler* database::task_scheduler() const noexcept {
    return task_scheduler_.get();
}

}

namespace jogasaki::api {

std::unique_ptr<database> create_database(std::shared_ptr<class configuration> cfg) {
    return std::make_unique<impl::database>(std::move(cfg));
}

}
