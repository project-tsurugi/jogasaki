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

#include <glog/logging.h>

#include <takatori/util/fail.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>

#include <jogasaki/logging.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/kvs/storage_dump.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/stealing_task_scheduler.h>
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
    init();
    if (! kvs_db_) {
        static constexpr std::string_view KEY_LOCATION{"location"};
        auto loc = cfg_->db_location();
        std::map<std::string, std::string> opts{};
        if (! loc.empty()) {
            opts.emplace(KEY_LOCATION, loc);
        }
        kvs_db_ = kvs::database::open(opts);
    }
    if (! kvs_db_) {
        LOG(ERROR) << "opening db failed";
        return status::err_io_error;
    }

    if(auto res = initialize_from_providers(); res != status::ok) {
        return res;
    }

    if (! task_scheduler_) {
        if (cfg_->single_thread()) {
            task_scheduler_ = std::make_shared<scheduler::serial_task_scheduler>();
        } else {
            task_scheduler_ = std::make_shared<scheduler::stealing_task_scheduler>(scheduler::thread_params(cfg_));
        }
    }
    task_scheduler_->start();
    return status::ok;
}

status database::stop() {
    task_scheduler_->stop();
    sequence_manager_.reset();

    if (kvs_db_) {
        if(! kvs_db_->close()) {
            return status::err_io_error;
        }
        kvs_db_ = nullptr;
    }
    return status::ok;
}

database::database(
    std::shared_ptr<class configuration> cfg
) :
    cfg_(std::move(cfg))
{
}

std::shared_ptr<class configuration> const& database::configuration() const noexcept {
    return cfg_;
}

database::database() : database(std::make_shared<class configuration>()) {}

void database::init() {
    executor::add_builtin_tables(*tables_);
    executor::add_test_tables(*tables_);  //TODO remove on production environment
    executor::add_qa_tables(*tables_);
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
    if(cfg_->prepare_analytics_benchmark_tables()) {
        executor::add_analytics_benchmark_tables(*tables_);
    }
    global::config_pool(cfg_);
}

void add_variable(
    yugawara::variable::configurable_provider& provider,
    std::string_view name,
    field_type_kind kind
) {
    // TODO find and add are thread-safe, but we need to them atomically
    if (auto e = provider.find(name)) {
        // ignore if it's already exists
        return;
    }
    switch(kind) {
        case field_type_kind::int4: provider.add({name, takatori::type::int4{}}, true); break;
        case field_type_kind::int8: provider.add({name, takatori::type::int8{}}, true); break;
        case field_type_kind::float4: provider.add({name, takatori::type::float4{}}, true); break;
        case field_type_kind::float8: provider.add({name, takatori::type::float8{}}, true); break;
        case field_type_kind::character: provider.add({name, takatori::type::character{takatori::type::varying}}, true); break;
        default: fail();
    }
}

status database::prepare_common(
    std::string_view sql,
    std::shared_ptr<yugawara::variable::configurable_provider> provider,
    std::unique_ptr<impl::prepared_statement>& statement
) {
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    ctx->variable_provider(std::move(provider));
    if(auto rc = plan::prepare(sql, *ctx); rc != status::ok) {
        VLOG(log_error) << "compilation failed.";
        return rc;
    }
    statement = std::make_unique<impl::prepared_statement>(ctx->prepared_statement());
    return status::ok;
}

status database::prepare_common(
    std::string_view sql,
    std::shared_ptr<yugawara::variable::configurable_provider> provider,
    statement_handle& statement
) {
    std::unique_ptr<impl::prepared_statement> ptr{};
    auto st = prepare_common(sql, std::move(provider), ptr);
    if (st == status::ok) {
        decltype(prepared_statements_)::accessor acc{};
        api::statement_handle handle{ptr.get()};
        if (prepared_statements_.insert(acc, handle)) {
            acc->second = std::move(ptr);
            statement = handle;
        } else {
            fail();
        }
    }
    return st;
}

status database::prepare(std::string_view sql, statement_handle& statement) {
    return prepare_common(sql, {}, statement);
}

status database::prepare(
    std::string_view sql,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    statement_handle& statement
) {
    auto host_variables = std::make_shared<yugawara::variable::configurable_provider>();
    for(auto&& [n, t] : variables) {
        add_variable(*host_variables, n, t);
    }
    return prepare_common(sql, std::move(host_variables), statement);
}

status database::create_executable(std::string_view sql, std::unique_ptr<api::executable_statement>& statement) {
    std::unique_ptr<impl::prepared_statement> prepared{};
    if(auto rc = prepare_common(sql, {}, prepared); rc != status::ok) {
        return rc;
    }
    std::unique_ptr<api::executable_statement> exec{};
    auto parameters = std::make_shared<impl::parameter_set>();
    if(auto rc = resolve_common(*prepared, parameters, exec); rc != status::ok) {
        return rc;
    }
    statement = std::make_unique<impl::executable_statement>(
        unsafe_downcast<impl::executable_statement>(*exec).body(),
        unsafe_downcast<impl::executable_statement>(*exec).resource(),
        maybe_shared_ptr<api::parameter_set const>{}
    );
    return status::ok;
}
status database::validate_option(transaction_option const& option) {
    if(option.is_long()) {
        for(auto&& wp : option.write_preserves()) {
            if(auto t = tables_->find_table(wp); ! t) {
                VLOG(log_error) << "The table `" << wp << "` specified for write preserve is not found.";
                return status::err_invalid_argument;
            }
        }
    }
    return status::ok;
}
status database::do_create_transaction(transaction_handle& handle, transaction_option const& option) {
    if (! kvs_db_) {
        VLOG(log_error) << "database not started";
        return status::err_invalid_state;
    }
    if(auto res = validate_option(option); res != status::ok) {
        return res;
    }
    {
        auto tx = std::make_unique<impl::transaction>(
            *this,
            option.readonly(),
            option.is_long(),
            option.write_preserves()
        );
        decltype(transactions_)::accessor acc{};
        api::transaction_handle t{tx.get()};
        if (transactions_.insert(acc, t)) {
            acc->second = std::move(tx);
            handle = t;
        } else {
            fail();
        }
    }
    return status::ok;
}

status database::resolve(
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::unique_ptr<api::executable_statement>& statement
) {
    return resolve_common(
        *reinterpret_cast<impl::prepared_statement*>(prepared.get()),  //NOLINT
        std::move(parameters),
        statement
    );
}

status database::resolve_common(
    impl::prepared_statement const& prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::unique_ptr<api::executable_statement>& statement
) {
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    auto& ps = unsafe_downcast<impl::prepared_statement>(prepared).body();
    ctx->variable_provider(ps->host_variables());
    ctx->prepared_statement(ps);
    auto params = unsafe_downcast<impl::parameter_set>(*parameters).body();
    if(auto rc = plan::compile(*ctx, params.get()); rc != status::ok) {
        VLOG(log_error) << "compilation failed.";
        return rc;
    }
    statement = std::make_unique<impl::executable_statement>(
        ctx->executable_statement(),
        std::move(resource),
        std::move(parameters)
    );
    return status::ok;
}

status database::destroy_statement(
    api::statement_handle prepared
) {
    decltype(prepared_statements_)::accessor acc{};
    if (prepared_statements_.find(acc, prepared)) {
        prepared_statements_.erase(acc);
    } else {
        VLOG(log_warning) << "destroy_statement for invalid handle";
        return status::not_found;
    }
    return status::ok;
}

status database::destroy_transaction(
    api::transaction_handle handle
) {
    decltype(transactions_)::accessor acc{};
    if (transactions_.find(acc, handle)) {
        transactions_.erase(acc);
    } else {
        VLOG(log_warning) << "destroy_statement for invalid handle";
        return status::not_found;
    }
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

status database::dump(std::ostream& output, std::string_view index_name, std::size_t batch_size) {
    kvs::storage_dump dumper{*kvs_db_};
    return dumper.dump(output, index_name, batch_size);
}

status database::load(std::istream& input, std::string_view index_name, std::size_t batch_size) {
    kvs::storage_dump dumper{*kvs_db_};
    return dumper.load(input, index_name, batch_size);
}

scheduler::task_scheduler* database::task_scheduler() const noexcept {
    return task_scheduler_.get();
}

status database::do_create_table(std::shared_ptr<yugawara::storage::table> table, std::string_view schema) {
    (void)schema;
    BOOST_ASSERT(table != nullptr);  //NOLINT
    std::string name{table->simple_name()};
    if (! kvs_db_) {
        VLOG(log_error) << "db not started";
        return status::err_invalid_state;
    }
    try {
        tables_->add_table(std::move(table));
    } catch(std::invalid_argument& e) {
        VLOG(log_error) << "table " << name << " already exists";
        return status::err_already_exists;
    }
    return status::ok;
}

std::shared_ptr<yugawara::storage::table const> database::do_find_table(
    std::string_view name,
    std::string_view schema
) {
    (void)schema;
    if(auto res = tables_->find_table(name)) {
        return res;
    }
    return {};
}

status database::do_drop_table(std::string_view name, std::string_view schema) {
    (void)schema;
    if(tables_->remove_relation(name)) {
        return status::ok;
    }
    return status::not_found;
}

status database::do_create_index(std::shared_ptr<yugawara::storage::index> index, std::string_view schema) {
    (void)schema;
    BOOST_ASSERT(index != nullptr);  //NOLINT
    std::string name{index->simple_name()};
    if (! kvs_db_) {
        VLOG(log_error) << "db not started";
        return status::err_invalid_state;
    }
    try {
        tables_->add_index(std::move(index));
    } catch(std::invalid_argument& e) {
        VLOG(log_error) << "index " << name << " already exists";
        return status::err_already_exists;
    }
    kvs_db_->create_storage(name); // Just to ensure existence of the storage. No need to handle return value.
    return status::ok;
}

std::shared_ptr<yugawara::storage::index const> database::do_find_index(
    std::string_view name,
    std::string_view schema
) {
    (void)schema;
    if(auto res = tables_->find_index(name)) {
        return res;
    }
    return {};
}

status database::do_drop_index(std::string_view name, std::string_view schema) {
    (void)schema;
    if(tables_->remove_index(name)) {
        // try to delete stroage on kvs.
        auto stg = kvs_db_->get_storage(name);
        if (! stg) {
            VLOG(log_info) << "kvs storage " << name << " not found.";
            return status::ok;
        }
        if(auto res = stg->delete_storage(); res != status::ok) {
            VLOG(log_error) << res << " error on deleting storage " << name;
        }
        return status::ok;
    }
    return status::not_found;
}

status database::do_create_sequence(std::shared_ptr<yugawara::storage::sequence> sequence, std::string_view schema) {
    (void)schema;
    BOOST_ASSERT(sequence != nullptr);  //NOLINT
    if (auto id = sequence->definition_id(); !id) {
        VLOG(log_error) << "The sequence definition id is not specified for sequence " <<
            sequence->simple_name() <<
            ". Specify definition id when creating the sequence.";
        return status::err_invalid_argument;
    }
    std::string name{sequence->simple_name()};
    if (! kvs_db_) {
        VLOG(log_error) << "db not started";
        return status::err_invalid_state;
    }
    try {
        tables_->add_sequence(std::move(sequence));
    } catch(std::invalid_argument& e) {
        VLOG(log_error) << "sequence " << name << " already exists";
        return status::err_already_exists;
    }
    return status::ok;
}

std::shared_ptr<yugawara::storage::sequence const>
database::do_find_sequence(std::string_view name, std::string_view schema) {
    (void)schema;
    if(auto res = tables_->find_sequence(name)) {
        return res;
    }
    return {};
}

status database::do_drop_sequence(std::string_view name, std::string_view schema) {
    (void)schema;
    if(tables_->remove_sequence(name)) {
        return status::ok;
    }
    return status::not_found;
}

executor::sequence::manager* database::sequence_manager() const noexcept {
    return sequence_manager_.get();
}

status database::initialize_from_providers() {
    bool success = true;
    tables_->each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const&) {
        success = success && kvs_db_->get_or_create_storage(id);
    });
    if (! success) {
        LOG(ERROR) << "creating table schema entries failed";
        return status::err_io_error;
    }
    sequence_manager_ = std::make_unique<executor::sequence::manager>(*kvs_db_);
    sequence_manager_->load_id_map();
    sequence_manager_->register_sequences(tables_);
    return status::ok;
}

std::shared_ptr<scheduler::task_scheduler> const& database::scheduler() const noexcept {
    return task_scheduler_;
}

std::shared_ptr<class configuration>& database::config() noexcept {
    return cfg_;
}

}

namespace jogasaki::api {

std::unique_ptr<database> create_database(std::shared_ptr<class configuration> cfg) {
    return std::make_unique<impl::database>(std::move(cfg));
}

}