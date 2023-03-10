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
#include <takatori/util/string_builder.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/decimal.h>

#include <jogasaki/logging.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/logship/log_event_listener.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/kvs/storage_dump.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/stealing_task_scheduler.h>
#include <jogasaki/scheduler/thread_params.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/utils/storage_metadata_serializer.h>
#include <jogasaki/utils/backoff_timer.h>
#include <jogasaki/utils/backoff_waiter.h>
#include <jogasaki/utils/proto_debug_string.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/constants.h>

#include <string_view>
#include <memory>
#include <takatori/serializer/json_printer.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/recovery/index.h>
#include <jogasaki/scheduler/task_factory.h>

#include "request_context_factory.h"

namespace jogasaki::api::impl {

using takatori::util::fail;
using takatori::util::string_builder;

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
    LOG(INFO) << "SQL engine configuration " << *cfg_;
    if (cfg_->quiescent()) {
        return status::ok;
    }
    init();
    if (! kvs_db_) {
        std::map<std::string, std::string> opts{};
        {
            static constexpr std::string_view KEY_LOCATION{"location"};
            auto loc = cfg_->db_location();
            if (! loc.empty()) {
                opts.emplace(KEY_LOCATION, loc);
            }
        }
        {
            static constexpr std::string_view KEY_LOGGING_MAX_PARALLELISM{"logging_max_parallelism"};
            auto sz = cfg_->max_logging_parallelism();
            if (sz > 0) {
                opts.emplace(KEY_LOGGING_MAX_PARALLELISM, std::to_string(sz));
            }
        }
        kvs_db_ = kvs::database::open(opts);
    }
    if (! kvs_db_) {
        LOG(ERROR) << "Opening database failed.";
        return status::err_io_error;
    }

    if(auto res = recover_metadata(); res != status::ok) {
        (void) kvs_db_->close();
        kvs_db_.reset();
        deinit();
        return res;
    }
    if(auto res = initialize_from_providers(); res != status::ok) {
        (void) kvs_db_->close();
        kvs_db_.reset();
        deinit();
        return res;
    }

    if (cfg_->activate_scheduler()) {
        if (! task_scheduler_) {
            if (cfg_->single_thread()) {
                task_scheduler_ = std::make_shared<scheduler::serial_task_scheduler>();
            } else {
                task_scheduler_ = std::make_shared<scheduler::stealing_task_scheduler>(scheduler::thread_params(cfg_));
            }
        }
        task_scheduler_->start();
    }

    if (cfg_->enable_logship()) {
        if (auto l = logship::create_log_event_listener(*cfg_, tables_)) {
            kvs_db_->log_event_listener(std::move(l));
            // ignore error for now TODO
        }
    }
    return status::ok;
}

status database::stop() {
    if (cfg_->quiescent()) {
        return status::ok;
    }
    if (cfg_->activate_scheduler()) {
        task_scheduler_->stop();
        task_scheduler_.reset();
    }
    sequence_manager_.reset();
    deinit();
    prepared_statements_.clear();

    if (kvs_db_) {
        if(! kvs_db_->close()) {
            return status::err_io_error;
        }

        // deinit event listener should come after kvs_db_->close() as it possibly sends last records on db shutdown
        if (cfg_->enable_logship()) {
            if (auto* l = kvs_db_->log_event_listener(); l != nullptr) {
                if(! l->deinit()) {
                    LOG(ERROR) << "shutting down log event listener failed.";
                    // even on error, proceed to shutdown all database
                }
            }
        }
        kvs_db_ = nullptr;
    }
    transactions_.clear();
    return status::ok;
}

database::database(
    std::shared_ptr<class configuration> cfg
) :
    cfg_(std::move(cfg))
{}

std::shared_ptr<class configuration> const& database::configuration() const noexcept {
    return cfg_;
}

database::database() : database(std::make_shared<class configuration>()) {}

void database::init() {
    global::config_pool(cfg_);
    if(initialized_) return;
    tables_ = std::make_shared<yugawara::storage::configurable_provider>();
    aggregate_functions_ = std::make_shared<yugawara::aggregate::configurable_provider>();
    executor::add_builtin_tables(*tables_);
    if(cfg_->prepare_test_tables()) {
        executor::add_test_tables(*tables_);
    }
    if(cfg_->prepare_qa_tables()) {
        executor::add_qa_tables(*tables_);
    }
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
    if(cfg_->prepare_phone_bill_tables()) {
        executor::add_phone_bill_tables(*tables_);
    }
    initialized_ = true;
}

void database::deinit() {
    if(! initialized_) return;
    tables_.reset();
    aggregate_functions_.reset();
    initialized_ = false;
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
        case field_type_kind::date: provider.add({name, takatori::type::date{}}, true); break;
        case field_type_kind::time_of_day: provider.add({name, takatori::type::time_of_day{}}, true); break;
        case field_type_kind::time_point: provider.add({name, takatori::type::time_point{}}, true); break;
        case field_type_kind::decimal: provider.add({name, takatori::type::decimal{}}, true); break;
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
    diagnostics_->clear();
    ctx->diag(*diagnostics_);
    if(auto rc = plan::prepare(sql, *ctx); rc != status::ok) {
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

std::vector<std::string> add_secondary_indices(
    std::vector<std::string> const& write_preserves,
    yugawara::storage::configurable_provider const& tables
) {
    std::vector<std::string> ret{};
    ret.reserve(write_preserves.size()*approx_index_count_per_table);
    for(auto&& wp : write_preserves) {
        auto t = tables.find_table(wp);
        if(! t) continue;
        tables.each_index([&](std::string_view , std::shared_ptr<yugawara::storage::index const> const& entry) {
            if(entry->table() == *t) {
                ret.emplace_back(entry->simple_name());
            }
        });
    }
    return ret;
}

kvs::transaction_option from(transaction_option const& option, yugawara::storage::configurable_provider const& tables) {
    auto type = kvs::transaction_option::transaction_type::occ;
    if(option.readonly()) {
        type = kvs::transaction_option::transaction_type::read_only;
    } else if (option.is_long()) {
        type = kvs::transaction_option::transaction_type::ltx;
    }
    return kvs::transaction_option{
        type,
        add_secondary_indices(option.write_preserves(), tables)
    };
}

status database::do_create_transaction(transaction_handle& handle, transaction_option const& option) {
    std::atomic_bool completed = false;
    status ret{status::ok};
    auto jobid = do_create_transaction_async([&handle, &completed, &ret](transaction_handle h, status st, std::string_view msg){
        completed = true;
        if(st != status::ok) {
            ret = st;
            VLOG(log_error) << "do_create_transaction failed with error : " << st << " " << msg;
            return;
        }
        handle = h;
    }, option);

    task_scheduler_->wait_for_progress(jobid);
    utils::backoff_waiter waiter{};
    while(! completed) {
        waiter();
    }
    return ret;
}
status database::create_transaction_internal(transaction_handle& handle, transaction_option const& option) {
    if (! kvs_db_) {
        VLOG(log_error) << "database not started";
        return status::err_invalid_state;
    }
    if(auto res = validate_option(option); res != status::ok) {
        return res;
    }
    {
        std::unique_ptr<impl::transaction> tx{};
        if(auto res = impl::transaction::create_transaction(*this, tx, from(option, *tables_)); res != status::ok) {
            return res;
        }
        api::transaction_handle t{tx.get()};
        {
            decltype(transactions_)::accessor acc{};
            if (transactions_.insert(acc, t)) {
                acc->second = std::move(tx);
                handle = t;
            } else {
                fail();
            }
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
    diagnostics_->clear();
    ctx->diag(*diagnostics_);
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
    std::uint64_t storage_id{kvs::database::undefined_storage_id};
    if(index->definition_id()) {
        storage_id = index->definition_id().value();
    }

    if (! kvs_db_) {
        VLOG(log_error) << "db not started";
        return status::err_invalid_state;
    }

    if(tables_->find_index(name)) {
        VLOG(log_error) << "index " << name << " already exists";
        return status::err_already_exists;
    }

    std::string storage{};
    if(auto res = recovery::create_storage_option(*index, storage, utils::metadata_serializer_option{true}); ! res) {
        return status::err_already_exists;
    }

    auto target = std::make_shared<yugawara::storage::configurable_provider>();
    if(auto res = recovery::deserialize_storage_option_into_provider(storage, *tables_, *target, false); ! res) {
        return status::err_unknown;
    }

    sharksfin::StorageOptions opt{storage_id};
    opt.payload(std::move(storage));
    if(auto stg = kvs_db_->create_storage(name, opt);! stg) {
        // something went wrong. Storage already exists. // TODO recreate storage with new storage option
        VLOG(log_warning) << "storage " << name << " already exists ";
        return status::err_unknown;
    }

    // only after successful update for kvs, merge metadata
    recovery::merge_deserialized_storage_option(*target, *tables_, true);
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
    if(! tables_->find_index(name)) {
        return status::not_found;
    }
    // try to delete stroage on kvs.
    auto stg = kvs_db_->get_storage(name);
    if (stg) {
        if(auto res = stg->delete_storage(); res != status::ok && res != status::not_found) {
            VLOG(log_error) << res << " error on deleting storage " << name;
            return status::err_unknown;
        }
    } else {
        // kvs storage is already removed somehow, let's proceed and remove from metadata.
        VLOG(log_info) << "kvs storage '" << name << "' not found.";
    }
    tables_->remove_index(name);
    return status::ok;
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
    {
        std::unique_ptr<kvs::transaction> tx{};
        if(auto res = kvs::transaction::create_transaction(*kvs_db_, tx); res != status::ok) {
            return res;
        }
        sequence_manager_->load_id_map(tx.get());
        sequence_manager_->register_sequences(tx.get(), tables_);
        if(auto res = tx->commit(); res != status::ok) {
            LOG(ERROR) << "committing table schema entries failed";
            sequence_manager_.reset();
            return status::err_io_error;
        }
    }
    return status::ok;
}

status database::recover_table(proto::metadata::storage::IndexDefinition const& idef) {
    utils::storage_metadata_serializer ser{};

    auto deserialized = std::make_shared<yugawara::storage::configurable_provider>();
    if(! ser.deserialize(idef, *tables_, *deserialized)) {
        return status::err_inconsistent_index;
    }

    return status::ok;
}

status database::recover_index_metadata(
    std::vector<std::string> const& keys,
    bool primary_only,
    std::vector<std::string>& skipped
) {
    skipped.clear();
    for(auto&& n : keys) {
        auto stg = kvs_db_->get_storage(n);
        if(! stg) {
            LOG(ERROR) << "Metadata recovery failed. Missing storage:" << n;
            return status::err_unknown;
        }
        sharksfin::StorageOptions opt{};
        if(auto res = stg->get_options(opt); res != status::ok) {
            return res;
        }
        auto payload = opt.payload();
        if(payload.empty()) {
            continue;
        }
        proto::metadata::storage::IndexDefinition idef{};
        if(! recovery::validate_extract(payload, idef)) {
            LOG(ERROR) << "Metadata recovery failed. Invalid metadata";
            return status::err_unknown;
        }
        if(primary_only && ! idef.has_table_definition()) {
            skipped.emplace_back(n);
            continue;
        }
        VLOG(log_info) << "Recover table/index " << n << " : " << utils::to_debug_string(idef);
        if(! recovery::deserialize_into_provider(idef, *tables_, *tables_, false)) {
            LOG(ERROR) << "Metadata recovery failed. Invalid metadata";
            return status::err_unknown;
        }
    }
    return status::ok;
}

status database::recover_metadata() {
    std::vector<std::string> names{};
    if(auto res = kvs_db_->list_storages(names); res != status::ok) {
        return res;
    }
    std::vector<std::string> secondaries{};
    secondaries.reserve(names.size());
    // recover primary index/table
    if(auto res = recover_index_metadata(names, true, secondaries); res != status::ok) {
        return res;
    }
    // recover secondaries
    std::vector<std::string> skipped{};
    if(auto res = recover_index_metadata(secondaries, false, skipped); res != status::ok) {
        return res;
    }
    return status::ok;
}

std::shared_ptr<scheduler::task_scheduler> const& database::scheduler() const noexcept {
    return task_scheduler_;
}

std::shared_ptr<class configuration>& database::config() noexcept {
    return cfg_;
}

database::database(std::shared_ptr<class configuration> cfg, sharksfin::DatabaseHandle db) :
    cfg_(std::move(cfg)),
    kvs_db_(std::make_unique<kvs::database>(db))
{}

std::shared_ptr<diagnostics> database::fetch_diagnostics() noexcept {
    if (! diagnostics_) {
        diagnostics_ = std::make_shared<diagnostics>();
    }
    return diagnostics_;
}

void submit_task_begin_wait(request_context* rctx, scheduler::task_body_type&& body) {
    auto t = scheduler::create_custom_task(rctx, std::move(body), true, true);
    auto& ts = *rctx->scheduler();
    ts.schedule_task(std::move(t));
}

constexpr static std::string_view log_location_prefix_timing_start_tx = "/:jogasaki:timing:start_transaction";

scheduler::job_context::job_id_type database::do_create_transaction_async(
    create_transaction_callback on_completion,
    transaction_option const& option
) {
    auto rctx = impl::create_request_context(
        this,
        nullptr,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool())
    );

    auto timer = std::make_shared<utils::backoff_timer>();
    auto handle = std::make_shared<transaction_handle>();
    auto t = scheduler::create_custom_task(rctx.get(),
        [this, rctx, option, handle, timer=std::move(timer)]() {
            auto res = create_transaction_internal(*handle, option);
            if(res != status::ok) {
                rctx->status_code(res,
                    string_builder{} << "creating transaction failed with error:" << res << string_builder::to_string
                );
                scheduler::submit_teardown(*rctx);
                return model::task_result::complete;
            }
            rctx->status_code(res);
            if(! option.is_long() && ! option.readonly()) {
                scheduler::submit_teardown(*rctx);
                return model::task_result::complete;
            }
            timer->reset();
            submit_task_begin_wait(rctx.get(), [rctx, handle, timer]() {
                if(! (*timer)()) return model::task_result::yield;
                if(handle->is_ready()) {
                    scheduler::submit_teardown(*rctx);
                    return model::task_result::complete;
                }
                return model::task_result::yield;
            });
            return model::task_result::complete;
        }, false);  // create transaction is not sticky task while its waiting task is.
    auto jobid = rctx->job()->id();
    rctx->job()->callback([on_completion=std::move(on_completion), rctx, handle, jobid](){
        VLOG(log_debug_timing_event) << log_location_prefix_timing_start_tx
            << " "
            << (*handle ? handle->transaction_id() : "<tx id not available>")
            << " job("
            << utils::hex(jobid)
            << ") to start transaction completed";
        on_completion(*handle, rctx->status_code(), rctx->status_message());
    });
    auto& ts = *rctx->scheduler();
    VLOG(log_debug_timing_event) << log_location_prefix_timing_start_tx
        << " job("
        << utils::hex(jobid)
        << ") to start transaction will be submitted";
    ts.schedule_task(std::move(t));
    return jobid;
}

void database::print_diagnostic(std::ostream &os) {
    os << "/:jogasaki print diagnostics start" << std::endl;
    if(task_scheduler_) {
        task_scheduler_->print_diagnostic(os);
    }
    os << "/:jogasaki print diagnostics end" << std::endl;
}

std::string database::diagnostic_string() {
    std::stringstream ss{};
    print_diagnostic(ss);
    return ss.str();
}

}

namespace jogasaki::api {

std::unique_ptr<database> create_database(std::shared_ptr<class configuration> cfg) {
    return std::make_unique<impl::database>(std::move(cfg));
}

std::unique_ptr<database> create_database(std::shared_ptr<configuration> cfg, sharksfin::DatabaseHandle db) {
    return std::make_unique<impl::database>(std::move(cfg), db);
}
}