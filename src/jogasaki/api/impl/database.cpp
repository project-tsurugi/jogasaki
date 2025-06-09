/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/serializer/json_printer.h>
#include <takatori/serializer/object_scanner.h>
#include <takatori/type/blob.h>
#include <takatori/type/character.h>
#include <takatori/type/clob.h>
#include <takatori/type/data.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/exception.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/reference_list_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/compiled_info.h>
#include <yugawara/serializer/object_scanner.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/relation.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/commit_stats.h>
#include <jogasaki/api/impl/error_info.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/statement_handle.h>
#include "jogasaki/api/statement_handle_internal.h"
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/durability_callback.h>
#include <jogasaki/durability_manager.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/batch/batch_execution_info.h>
#include <jogasaki/executor/batch/batch_execution_state.h>
#include <jogasaki/executor/batch/batch_executor.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/executor/function/builtin_scalar_functions.h>
#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/external_log/event_logging.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/storage_dump.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/model/task.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/executable_statement.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/recovery/index.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_logging.h>
#include <jogasaki/scheduler/conditional_task.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/hybrid_task_scheduler.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/stealing_task_scheduler.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/scheduler/thread_params.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/backoff_waiter.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/external_log_utils.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/utils/proto_debug_string.h>
#include <jogasaki/utils/storage_metadata_serializer.h>
#include <jogasaki/utils/string_manipulation.h>
#include <jogasaki/utils/use_counter.h>
#include <jogasaki/utils/validate_index_key_type.h>
#include <jogasaki/utils/validate_table_definition.h>

#include "request_context_factory.h"

namespace jogasaki::api::impl {

using takatori::util::string_builder;
using takatori::util::throw_exception;

constexpr static std::string_view log_location_prefix = "/:jogasaki:api:impl:database ";

std::shared_ptr<kvs::database> const& database::kvs_db() const noexcept {
    return kvs_db_;
}

std::shared_ptr<yugawara::storage::configurable_provider> const& database::tables() const noexcept {
    return tables_;
}

std::shared_ptr<yugawara::aggregate::configurable_provider> const& database::aggregate_functions() const noexcept {
    return aggregate_functions_;
}

#define LOGCFG (LOG(INFO) << lp << std::boolalpha)
void dump_public_configurations(configuration const& cfg) {
    constexpr std::string_view lp = "/:jogasaki:config: ";
    LOGCFG << "(thread_pool_size) " << cfg.thread_pool_size() << " : number of threads used by task scheduler";
    LOGCFG << "(enable_index_join) " << cfg.enable_index_join() << " : whether join tries to use index";
    LOGCFG << "(stealing_enabled) " << cfg.stealing_enabled() << " : whether task scheduler steals tasks";
    LOGCFG << "(default_partitions) " << cfg.default_partitions() << " : number of default partitions for relational operators";
    LOGCFG << "(use_preferred_worker_for_current_thread) " << cfg.use_preferred_worker_for_current_thread() << " : whether to use fixed worker assigned for request thread";
    LOGCFG << "(stealing_wait) " << cfg.stealing_wait() << " : number of polling by worker thread on task queue before stealing";
    LOGCFG << "(task_polling_wait) " << cfg.task_polling_wait() << " : sleep duration(us) of worker thread that find no task";
    LOGCFG << "(enable_hybrid_scheduler) " << cfg.enable_hybrid_scheduler() << " : whether to enable hybrid scheduler";
    LOGCFG << "(lightweight_job_level) " << cfg.lightweight_job_level() << " : boundary value to define job that finishes quickly";
    LOGCFG << "(busy_worker) " << cfg.busy_worker() << " : whether task scheduler workers check task queues highly frequently";
    LOGCFG << "(watcher_interval) " << cfg.watcher_interval() << " : duration(us) between watcher thread suspends and resumes";
    LOGCFG << "(worker_try_count) " << cfg.worker_try_count() << " : number of polling by worker thread on task queue before suspend";
    LOGCFG << "(worker_suspend_timeout) " << cfg.worker_suspend_timeout() << " : duration(us)  between worker thread suspends and resumes";
    LOGCFG << "(commit_response) " << cfg.default_commit_response() << " : commit notification timing default";
    LOGCFG << "(dev_update_skips_deletion) " << cfg.update_skips_deletion() << " : whether update statement skips unnecessary deletion when possible";
    LOGCFG << "(dev_profile_commits) " << cfg.profile_commits() << " : whether to profile commit/durability callbacks";
    LOGCFG << "(dev_return_os_pages) " << cfg.return_os_pages() << " : whether to return released memory pages to operating system";
    LOGCFG << "(dev_omit_task_when_idle) " << cfg.omit_task_when_idle() << " : whether to stop scheduling tasks to process durability callback if there is no transaction waiting for durable";
    LOGCFG << "(plan_recording) " << cfg.plan_recording() << " : whether altimeter to output stmt_explain event log";
    LOGCFG << "(dev_try_insert_on_upserting_secondary) " << cfg.try_insert_on_upserting_secondary() << " : whether to try insert first when INSERT OR REPLACE is exected for tables with secondary index";
    LOGCFG << "(dev_scan_concurrent_operation_as_not_found) " << cfg.scan_concurrent_operation_as_not_found() << " : whether scan to treat status::concurrent_operation as status::not_found";
    LOGCFG << "(dev_point_read_concurrent_operation_as_not_found) " << cfg.point_read_concurrent_operation_as_not_found() << " : whether point read to treat status::concurrent_operation as status::not_found";
    LOGCFG << "(dev_lowercase_regular_identifiers) " << cfg.lowercase_regular_identifiers() << " : whether to lowercase regular identifiers";
    LOGCFG << "(zone_offset) " << cfg.zone_offset() << " : system time zone offset in minutes";
    LOGCFG << "(scan_block_size) " << cfg.scan_block_size() << " : max records processed by scan operator before yielding to other task";
    LOGCFG << "(scan_yield_interval) " << cfg.scan_yield_interval() << " : max time (ms) processed by scan operator before yielding to other tasks";
    LOGCFG << "(dev_thousandths_ratio_check_local_first) " << cfg.thousandths_ratio_check_local_first() << " : how frequently (represented as count out of 1000 executions) task scheduler checks local task queue first";
    LOGCFG << "(dev_direct_commit_callback) " << cfg.direct_commit_callback() << " : whether to make callback directly from shirakami to client on pre-commit response (only for `available` and `accepted`)";
    LOGCFG << "(scan_default_parallel) " << cfg.scan_default_parallel() << " : max parallel execution count of scan tasks";
    LOGCFG << "(dev_inplace_teardown) " << cfg.inplace_teardown() << " : whether to process teardown (job completion) directly on the current thread instead of scheduling a task for it";
    LOGCFG << "(enable_join_scan) " << cfg.enable_join_scan() << " : whether to enable index join using join_scan operator";
    LOGCFG << "(dev_rtx_key_distribution) " << cfg.key_distribution() << " : key distribution policy used for RTX parallel scan";
    LOGCFG << "(dev_enable_blob_cast) " << cfg.enable_blob_cast() << " : whether to enable cast expression to/from blob/clob data";
    LOGCFG << "(max_result_set_writers) " << cfg.max_result_set_writers() << " : max number of result set writers";
    LOGCFG << "(dev_core_affinity) " << cfg.core_affinity() << " : whether to assign cores to worker threads";
    LOGCFG << "(dev_initial_core) " << cfg.initial_core() << " : the initial core (0-origin) that core assign begins with sequentially";
    LOGCFG << "(dev_assign_numa_nodes_uniformly) " << cfg.assign_numa_nodes_uniformly() << " : whether to assign nodes to worker threads uniformly";
    LOGCFG << "(dev_force_numa_node) " << cfg.force_numa_node() << " : whether to assign the single node to all worker threads";
}

bool validate_core_assignment_parameters(configuration const& cfg) {
    if(cfg.core_affinity() && (cfg.assign_numa_nodes_uniformly() || cfg.force_numa_node() != configuration::numa_node_unspecified)) {
        // core assign and node assign cannot be set simultaneously
        return false;
    }
    if (cfg.assign_numa_nodes_uniformly() && cfg.force_numa_node() != configuration::numa_node_unspecified) {
        // uniform numa nodes and force_numa_node are mutually exclusive
        return false;
    }
    if (cfg.core_affinity() && cfg.initial_core() + cfg.thread_pool_size() > std::thread::hardware_concurrency()) {
        // the largest core index must not go over the maximum
        return false;
    }
    return true;
}

status database::start() {
    LOG_LP(INFO) << "SQL engine configuration " << *cfg_;
    dump_public_configurations(*cfg_);

    if (! validate_core_assignment_parameters(*cfg_)) {
        LOG_LP(ERROR) << std::boolalpha
            << "invalid core assignment configuration core_affinity:" << cfg_->core_affinity()
            << " assign_numa_nodes_uniformly:" << cfg_->assign_numa_nodes_uniformly()
            << " force_numa_node:" << cfg_->force_numa_node()
            << " thread_pool_size:" << cfg_->thread_pool_size()
            << " #cores:" << std::thread::hardware_concurrency(); // logical cores
        return status::err_io_error;
    }

    // this function is not called on maintenance/quiescent mode
    init();
    if (! kvs_db_) {
        // This is for dev/test. In production, kvs db is created outside.
        std::map<std::string, std::string> opts{};
        {
            static constexpr std::string_view KEY_LOCATION{"location"};
            auto loc = cfg_->db_location();
            if (! loc.empty()) {
                opts.emplace(KEY_LOCATION, loc);
            }
        }
        kvs_db_ = kvs::database::open(opts);
    }
    if (! kvs_db_) {
        LOG_LP(ERROR) << "Opening database failed.";
        return status::err_io_error;
    }
    if(auto res = setup_system_storage(); res != status::ok) {
        (void) kvs_db_->close();
        kvs_db_.reset();
        deinit();
        return res;
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
            } else if(cfg_->enable_hybrid_scheduler()) {
                task_scheduler_ = std::make_shared<scheduler::hybrid_task_scheduler>(scheduler::thread_params(cfg_));
            } else {
                task_scheduler_ = std::make_shared<scheduler::stealing_task_scheduler>(scheduler::thread_params(cfg_));
            }
        }
        task_scheduler_->start();
    }

    commit_stats_->enabled(cfg_->profile_commits());
    kvs_db_->register_durability_callback(durability_callback{*this});

    stop_requested_ = false;
    return status::ok;
}

status database::stop() {
    stop_requested_ = true;
    std::size_t cnt = 0;
    while(requests_inprocess_.count() != 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        if(++cnt > 1000) {
            LOG_LP(ERROR) << "Request to stop engine timed out.";
            return status::err_time_out;
        }
    }
    // this function is not called on maintenance/quiescent mode
    if (cfg_->activate_scheduler()) {
        task_scheduler_->stop();
        task_scheduler_.reset();
    }
    sequence_manager_.reset();

    global::page_pool().unsafe_dump_info(LOG(INFO) << log_location_prefix << "Memory pool statistics ");
    deinit();
    prepared_statements_.clear();
    statement_stores_.clear();

    transactions_.clear();
    transaction_stores_.clear();
    if (kvs_db_) {
        if(! kvs_db_->close()) {
            return status::err_io_error;
        }
        kvs_db_ = nullptr;
    }

    commit_stats_->dump();
    return status::ok;
}

void custom_external_log_cfg(std::shared_ptr<class configuration> const& cfg) {
    (void) cfg;
#ifdef ENABLE_ALTIMETER
    if(cfg) {
        cfg->trace_external_log(true);
    }
#endif
}

database::database(
    std::shared_ptr<class configuration> cfg
) :
    cfg_(std::move(cfg))
{
    custom_external_log_cfg(cfg_);
}

database::database(std::shared_ptr<class configuration> cfg, sharksfin::DatabaseHandle db) :
    cfg_(std::move(cfg)),
    kvs_db_(std::make_shared<kvs::database>(db))
{
    custom_external_log_cfg(cfg_);
    global::db(kvs_db_);
}

std::shared_ptr<class configuration> const& database::configuration() const noexcept {
    return cfg_;
}

database::database() : database(std::make_shared<class configuration>()) {}

void database::init() {
    global::config_pool(cfg_);
    if(initialized_) return;
    tables_ = std::make_shared<yugawara::storage::configurable_provider>();
    scalar_functions_ = global::scalar_function_provider(std::make_shared<yugawara::function::configurable_provider>());
    executor::function::add_builtin_scalar_functions(
        *scalar_functions_,
        global::scalar_function_repository()
    );
    aggregate_functions_ = std::make_shared<yugawara::aggregate::configurable_provider>();
    executor::function::incremental::add_builtin_aggregate_functions(
        *aggregate_functions_,
        global::incremental_aggregate_function_repository()
    );
    executor::function::add_builtin_aggregate_functions(
        *aggregate_functions_,
        global::aggregate_function_repository()
    );
    if(cfg_->prepare_analytics_benchmark_tables()) {
        executor::add_analytics_benchmark_tables(*tables_);
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
        case field_type_kind::boolean: provider.add({name, takatori::type::boolean{}}, true); break;
        case field_type_kind::int4: provider.add({name, takatori::type::int4{}}, true); break;
        case field_type_kind::int8: provider.add({name, takatori::type::int8{}}, true); break;
        case field_type_kind::float4: provider.add({name, takatori::type::float4{}}, true); break;
        case field_type_kind::float8: provider.add({name, takatori::type::float8{}}, true); break;
        case field_type_kind::decimal: provider.add({name, takatori::type::decimal{}}, true); break;
        case field_type_kind::character: provider.add({name, takatori::type::character{takatori::type::varying}}, true); break;
        case field_type_kind::octet: provider.add({name, takatori::type::octet{takatori::type::varying}}, true); break;
        case field_type_kind::date: provider.add({name, takatori::type::date{}}, true); break;
        case field_type_kind::time_of_day: provider.add({name, takatori::type::time_of_day{}}, true); break;
        case field_type_kind::time_of_day_with_time_zone: provider.add({name, takatori::type::time_of_day{takatori::type::with_time_zone}}, true); break;
        case field_type_kind::time_point: provider.add({name, takatori::type::time_point{}}, true); break;
        case field_type_kind::time_point_with_time_zone: provider.add({name, takatori::type::time_point{takatori::type::with_time_zone}}, true); break;
        case field_type_kind::blob: provider.add({name, takatori::type::blob{}}, true); break;
        case field_type_kind::clob: provider.add({name, takatori::type::clob{}}, true); break;
        default:
            throw_exception(std::logic_error{""});
    }
}

status database::prepare_common(
    std::string_view sql,
    std::shared_ptr<yugawara::variable::configurable_provider> provider,
    std::unique_ptr<impl::prepared_statement>& statement,
    std::shared_ptr<error::error_info>& out,
    plan::compile_option const& option
) {
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::prepare);
    req->statement_text(std::make_shared<std::string>(sql)); //TODO want to use shared_ptr created in plan::prepare
    req->status(scheduler::request_detail_status::accepted);
    log_request(*req);
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    ctx->function_provider(scalar_functions_);
    ctx->variable_provider(std::move(provider));
    ctx->option(option);
    if(auto rc = plan::prepare(sql, *ctx); rc != status::ok) {
        req->status(scheduler::request_detail_status::finishing);
        log_request(*req, false);
        out = ctx->error_info();
        return rc;
    }
    statement = std::make_unique<impl::prepared_statement>(ctx->prepared_statement());
    req->status(scheduler::request_detail_status::finishing);
    log_request(*req);
    return status::ok;
}

status database::prepare_common(
    std::string_view sql,
    std::shared_ptr<yugawara::variable::configurable_provider> provider,
    statement_handle& statement,
    std::shared_ptr<error::error_info>& out,
    plan::compile_option const& option
) {
    std::unique_ptr<impl::prepared_statement> ptr{};
    auto st = prepare_common(sql, std::move(provider), ptr, out, option);
    if (st == status::ok) {
        api::statement_handle handle{ptr.get(), option.session_id()};
        if (! handle.session_id().has_value()) {
            decltype(prepared_statements_)::accessor acc{};
            if (prepared_statements_.insert(acc, handle)) {
                acc->second = std::move(ptr);
                statement = handle;
            } else {
                throw_exception(std::logic_error{""});
            }
        } else {
            auto session_id = option.session_id().value();
            decltype(statement_stores_)::accessor acc{};
            auto new_item = statement_stores_.insert(acc, session_id);
            if (new_item) {
                acc->second = std::make_shared<statement_store>(session_id);
            }
            if (! acc->second->put(handle, std::move(ptr))) {
                throw_exception(std::logic_error{""});
            }
            statement = handle;
        }
    }
    return st;
}

status database::prepare(std::string_view sql, statement_handle& statement) {
    std::shared_ptr<error::error_info> info{};
    return prepare(sql, statement, info);
}

status database::prepare(
    std::string_view sql,
    statement_handle& statement,
    std::shared_ptr<error::error_info>& out,
    plan::compile_option const& option
) {
    return prepare_common(sql, {}, statement, out, option);
}

status database::prepare(
    std::string_view sql,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    statement_handle& statement
) {
    std::shared_ptr<error::error_info> info{};
    return prepare(sql, variables, statement, info, {});
}

status database::prepare(
    std::string_view sql,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    statement_handle& statement,
    std::shared_ptr<error::error_info>& out,
    plan::compile_option const& option
) {
    auto host_variables = std::make_shared<yugawara::variable::configurable_provider>();
    for(auto&& [n, t] : variables) {
        add_variable(*host_variables, n, t);
    }
    return prepare_common(sql, std::move(host_variables), statement, out, option);
}

status database::create_executable(std::string_view sql, std::unique_ptr<api::executable_statement>& statement) {
    std::shared_ptr<error::error_info> info{};
    return create_executable(sql, statement, info);
}

status database::create_executable(
    std::string_view sql,
    std::unique_ptr<api::executable_statement>& statement,
    std::shared_ptr<error::error_info>& out,
    plan::compile_option const& option
) {
    std::unique_ptr<impl::prepared_statement> prepared{};
    if(auto rc = prepare_common(sql, {}, prepared, out, option); rc != status::ok) {
        return rc;
    }
    std::unique_ptr<api::executable_statement> exec{};
    auto parameters = std::make_shared<impl::parameter_set>();
    if(auto rc = resolve_common(*prepared, parameters, exec, out); rc != status::ok) {
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
                VLOG_LP(log_error) << "The table `" << wp << "` specified for write preserve is not found.";
                return status::err_invalid_argument;
            }
        }
        for(auto&& rae : option.read_areas_exclusive()) {
            if(auto t = tables_->find_table(rae); ! t) {
                VLOG_LP(log_error) << "The table `" << rae << "` specified for exclusive read area is not found.";
                return status::err_invalid_argument;
            }
        }
        for(auto&& rai : option.read_areas_inclusive()) {
            if(auto t = tables_->find_table(rai); ! t) {
                VLOG_LP(log_error) << "The table `" << rai << "` specified for inclusive read area is not found.";
                return status::err_invalid_argument;
            }
        }
    }
    return status::ok;
}

void add_system_tables(
    std::vector<std::string>& write_preserves
) {
    write_preserves.emplace_back(system_sequences_name);
}

std::vector<std::string> add_wp_to_read_area_inclusive(
    std::vector<std::string> const& write_preserves,
    std::vector<std::string> const& read_areas_inclusive
) {
    if(read_areas_inclusive.empty()) {
        // Any table is readable. No need to add wps.
        return {};
    }
    std::set<std::string> rai{read_areas_inclusive.begin(), read_areas_inclusive.end()}; //std::set to remove duplicate
    for(auto&& wp : write_preserves) {
        rai.emplace(wp);
    }
    std::vector<std::string> ret{};
    ret.reserve(rai.size());
    for(auto&& ra : rai) {
        ret.emplace_back(ra);
    }
    return ret;
}

std::vector<std::string> add_secondary_indices(
    std::vector<std::string> const& table_areas,
    yugawara::storage::configurable_provider const& tables
) {
    std::vector<std::string> ret{};
    ret.reserve(table_areas.size()*approx_index_count_per_table);
    for(auto&& ta : table_areas) {
        auto t = tables.find_table(ta);
        if(! t) continue;
        tables.each_index([&](std::string_view , std::shared_ptr<yugawara::storage::index const> const& entry) {
            if(entry->table() == *t) {
                ret.emplace_back(entry->simple_name());
            }
        });
    }
    return ret;
}

std::shared_ptr<api::transaction_option> modify_ras_wps(transaction_option const& option, yugawara::storage::configurable_provider const& tables) {
    // add system tables to wp if modifies_definitions=true
    auto* wps = std::addressof(option.write_preserves());
    std::vector<std::string> with_system_tables{};
    if(option.modifies_definitions() && option.is_long()) {
        // this is done only for ltx, otherwise passing wps will be an error on cc engine
        with_system_tables = option.write_preserves();
        add_system_tables(with_system_tables);
        wps = std::addressof(with_system_tables);
    }
    // SQL IUD almost always (except INSERT OR REPLACE) require read semantics, so write preserve will be added to rai.
    std::vector<std::string> rai{add_wp_to_read_area_inclusive(*wps, option.read_areas_inclusive())};
    return std::make_shared<api::transaction_option>(
        option.type(),
        add_secondary_indices(*wps, tables),
        option.label(),
        add_secondary_indices(rai, tables),
        add_secondary_indices(option.read_areas_exclusive(), tables),
        option.modifies_definitions(),
        option.scan_parallel(),
        option.session_id()
    );
}

status database::do_create_transaction(transaction_handle& handle, transaction_option const& option) {
    std::shared_ptr<api::error_info> out{};
    return do_create_transaction(handle, option, out);
}

status database::do_create_transaction(transaction_handle& handle, transaction_option const& option, std::shared_ptr<api::error_info>& out) {
    std::atomic_bool completed = false;
    status ret{status::ok};
    auto jobid = do_create_transaction_async(
        [&handle, &completed, &ret, &out](
            transaction_handle h,
            status st,
            std::shared_ptr<api::error_info> info  //NOLINT(performance-unnecessary-value-param)
        ) {
            completed = true;
            out = info;
            if(st != status::ok) {
                ret = st;
                VLOG(log_error) << log_location_prefix << "do_create_transaction failed with error : " << info->code()
                                << " " << info->message();
                return;
            }
            handle = h;
        },
        option,
        {}
    );

    task_scheduler_->wait_for_progress(jobid);
    utils::backoff_waiter waiter{};
    while(! completed) {
        waiter();
    }
    return ret;
}
status database::create_transaction_internal(std::shared_ptr<transaction_context>& out, transaction_option const& option) {
    if (! kvs_db_) {
        VLOG_LP(log_error) << "database not started";
        return status::err_invalid_state;
    }
    if(auto res = validate_option(option); res != status::ok) {
        return res;
    }
    {
        std::shared_ptr<transaction_context> tx{};
        if(auto res = executor::create_transaction(tx, modify_ras_wps(option, *tables_)); res != status::ok) {
            return res;
        }
        tx->label(option.label());
        api::transaction_handle t{tx->surrogate_id(), option.session_id().has_value() ? std::optional<std::size_t>{option.session_id().value()} : std::nullopt};
        out = std::move(tx);
        if (! option.session_id().has_value()) {
            decltype(transactions_)::accessor acc{};
            if (transactions_.insert(acc, t)) {
                acc->second = out;
            } else {
                throw_exception(std::logic_error{""});
            }
        } else {
            auto session_id = option.session_id().value();
            decltype(transaction_stores_)::accessor acc{};
            auto new_item = transaction_stores_.insert(acc, session_id);
            if (new_item) {
                acc->second = std::make_shared<transaction_store>(session_id);
            }
            if (! acc->second->put(t, out)) {
                throw_exception(std::logic_error{""});
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
    std::shared_ptr<error::error_info> info{};
    return resolve(prepared, parameters, statement, info);
}

status database::resolve(
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::unique_ptr<api::executable_statement>& statement,
    std::shared_ptr<error::error_info>& out
) {
    auto stmt = get_statement(prepared);
    if (stmt == nullptr) {
        auto m = string_builder{} << "prepared statement not found handle:" << prepared << string_builder::to_string;
        auto rc = status::err_invalid_argument;
        out = create_error_info(
            error_code::statement_not_found_exception,
            m,
            rc
        );
        return rc;
    }
    return resolve_common(
        *stmt,
        std::move(parameters),
        statement,
        out
    );
}

status database::resolve_common(
    impl::prepared_statement const& stmt,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::unique_ptr<api::executable_statement>& statement,
    std::shared_ptr<error::error_info>& out
) {
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool());
    auto ctx = std::make_shared<plan::compiler_context>();
    ctx->resource(resource);
    ctx->storage_provider(tables_);
    ctx->aggregate_provider(aggregate_functions_);
    ctx->function_provider(scalar_functions_);
    auto& ps = stmt.body();
    ctx->variable_provider(ps->host_variables());
    ctx->prepared_statement(ps);
    auto params = unsafe_downcast<impl::parameter_set>(*parameters).body();
    if(auto rc = plan::compile(*ctx, params.get()); rc != status::ok) {
        VLOG_LP(log_error) << "compilation failed.";
        out = ctx->error_info();
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
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::dispose_statement);
    req->status(scheduler::request_detail_status::accepted);
    log_request(*req);
    if (! prepared.session_id().has_value()) {
        decltype(prepared_statements_)::accessor acc{};
        if (prepared_statements_.find(acc, prepared)) {
            prepared_statements_.erase(acc);
            req->status(scheduler::request_detail_status::finishing);
            log_request(*req);
            return status::ok;
        }
        VLOG_LP(log_warning) << "destroy_statement for invalid handle";
        req->status(scheduler::request_detail_status::finishing);
        log_request(*req, false);
        return status::err_invalid_argument;
    }
    decltype(statement_stores_)::accessor acc{};
    if (statement_stores_.find(acc, prepared.session_id().value())) {
        if (acc->second->remove(prepared)) {
            req->status(scheduler::request_detail_status::finishing);
            log_request(*req);
            return status::ok;
        }
    }
    VLOG_LP(log_warning) << "destroy_statement for invalid handle";
    req->status(scheduler::request_detail_status::finishing);
    log_request(*req, false);
    return status::err_invalid_argument;
}

status database::destroy_transaction(
    api::transaction_handle handle
) {
    if (! handle.session_id().has_value()) {
        decltype(transactions_)::accessor acc{};
        if (transactions_.find(acc, handle)) {
            if(cfg_->profile_commits()) {
                commit_stats_->add(*acc->second->profile());
            }
            transactions_.erase(acc);
            return status::ok;
        }
        VLOG_LP(log_warning) << "invalid handle";
        return status::err_invalid_argument;
    }
    decltype(transaction_stores_)::accessor acc{};
    if (transaction_stores_.find(acc, handle.session_id().value())) {
        if (acc->second->remove(handle)) {
            return status::ok;
        }
    }
    return status::err_invalid_argument;
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
    // request context is just to call validate_table_definition and receive error info
    auto context = impl::create_request_context(
        *this,
        nullptr,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        {},
        {}
    );
    if(! utils::validate_table_definition(*context, *table)) {
        return context->error_info()->status();
    }

    std::string name{table->simple_name()};
    if (! kvs_db_) {
        VLOG_LP(log_error) << "db not started";
        return status::err_invalid_state;
    }
    try {
        tables_->add_table(std::move(table));
    } catch(std::invalid_argument& e) {
        VLOG_LP(log_error) << "table " << name << " already exists";
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

bool validate_primary_key_nullability(yugawara::storage::index const& index) {
    if(index.simple_name() == index.table().simple_name()) {
        // primary index
        for(auto&& c : index.keys()) {
            if(c.column().criteria().nullity().nullable()) {
                VLOG_LP(log_error) << "primary key column \"" << c.column().simple_name() << "\" must not be nullable";
                return false;
            }
        }
    }
    return true;
}

status database::do_create_index(std::shared_ptr<yugawara::storage::index> index, std::string_view schema) {
    (void)schema;
    BOOST_ASSERT(index != nullptr);  //NOLINT
    std::string name{index->simple_name()};
    std::uint64_t storage_id{kvs::database::undefined_storage_id};
    if(index->definition_id()) {
        storage_id = index->definition_id().value();
    }
    if(! validate_primary_key_nullability(*index)) {
        return status::err_illegal_operation;
    }

    if (! kvs_db_) {
        VLOG_LP(log_error) << "db not started";
        return status::err_invalid_state;
    }

    if(tables_->find_index(name)) {
        VLOG_LP(log_error) << "index " << name << " already exists";
        return status::err_already_exists;
    }

    // request context is just to call validate_index_key_type and receive error info
    auto context = impl::create_request_context(
        *this,
        nullptr,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        {},
        {}
    );
    if(! utils::validate_index_key_type(*context, *index)) {
        return context->error_info()->status();
    }

    std::string storage{};
    if(auto err = recovery::create_storage_option(*index, storage, utils::metadata_serializer_option{true})) {
        if(! VLOG_IS_ON(log_trace)) {  // avoid duplicate log entry with log_trace
            VLOG_LP(log_error) << "error_info:" << *err;
        }
        return err->status();
    }

    auto target = std::make_shared<yugawara::storage::configurable_provider>();
    if(auto err = recovery::deserialize_storage_option_into_provider(storage, *tables_, *target, false)) {
        if(! VLOG_IS_ON(log_trace)) {  // avoid duplicate log entry with log_trace
            VLOG_LP(log_error) << "error_info:" << *err;
        }
        return err->status();
    }

    sharksfin::StorageOptions opt{storage_id};
    opt.payload(std::move(storage));
    if(auto stg = kvs_db_->create_storage(name, opt);! stg) {
        // something went wrong. Storage already exists. // TODO recreate storage with new storage option
        VLOG_LP(log_warning) << "storage " << name << " already exists ";
        return status::err_unknown;
    }

    // only after successful update for kvs, merge metadata
    if(auto err = recovery::merge_deserialized_storage_option(*target, *tables_, true)) {
        // normally the error should not happen because overwrite=true
        if(! VLOG_IS_ON(log_trace)) {  // avoid duplicate log entry with log_trace
            VLOG_LP(log_error) << "error_info:" << *err;
        }
        return err->status();
    }
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
            VLOG_LP(log_error) << res << " error on deleting storage " << name;
            return status::err_unknown;
        }
    } else {
        // kvs storage is already removed somehow, let's proceed and remove from metadata.
        VLOG_LP(log_info) << "kvs storage '" << name << "' not found.";
    }
    tables_->remove_index(name);
    return status::ok;
}

status database::do_create_sequence(std::shared_ptr<yugawara::storage::sequence> sequence, std::string_view schema) {
    (void)schema;
    BOOST_ASSERT(sequence != nullptr);  //NOLINT
    if (auto id = sequence->definition_id(); !id) {
        VLOG_LP(log_error) << "The sequence definition id is not specified for sequence " <<
            sequence->simple_name() <<
            ". Specify definition id when creating the sequence.";
        return status::err_invalid_argument;
    }
    std::string name{sequence->simple_name()};
    if (! kvs_db_) {
        VLOG_LP(log_error) << "db not started";
        return status::err_invalid_state;
    }
    try {
        tables_->add_sequence(std::move(sequence));
    } catch(std::invalid_argument& e) {
        VLOG_LP(log_error) << "sequence " << name << " already exists";
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
        LOG_LP(ERROR) << "creating table schema entries failed";
        return status::err_io_error;
    }
    sequence_manager_ = std::make_unique<executor::sequence::manager>(*kvs_db_);
    {
        std::unique_ptr<kvs::transaction> tx{};
        if(auto res = kvs::transaction::create_transaction(*kvs_db_, tx); res != status::ok) {
            return res;
        }
        try {
            sequence_manager_->load_id_map(tx.get());
            sequence_manager_->register_sequences(tx.get(), tables_, false);
        } catch(executor::sequence::exception& e) {
            if(e.get_status() != status::err_not_found) {
                LOG_LP(ERROR) << "registering sequences failed:" << e.get_status() << " " << e.what();
                return e.get_status();
            }
            // missing sequence entry in __sequences table
            // The situation possibly occurs by aborting transaction used for CREATE TABLE.
            // Dropping the table and recreating it will fix the issue.
            // We do not raise error in the start-up here. Allow users to read/dump the data for backup or
            // to drop the table to fix the situation.
            LOG_LP(WARNING) << "sequence '" << e.what()
                            << "' not found on the system table. Possibly the table definition did not "
                               "complete successfully. Inserting new records using the sequence is likely to hit an "
                               "error. Re-creating the table that owns the sequence may fix the issue";
        }
        if(auto res = tx->commit(); res != status::ok) {
            LOG_LP(ERROR) << "committing table schema entries failed";
            sequence_manager_.reset();
            return status::err_io_error;
        }
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
            LOG_LP(ERROR) << "Metadata recovery failed. Missing storage:" << n;
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
        std::uint64_t v{};
        if(auto err = recovery::validate_extract(payload, idef, v)) {
            LOG_LP(ERROR) << "Metadata recovery failed. Invalid metadata: " << *err;
            return err->status();
        }
        if(primary_only && ! idef.has_table_definition()) {
            skipped.emplace_back(n);
            continue;
        }
        LOG_LP(INFO) << "Recovering metadata \"" << n << "\" (v=" << v << ") : " << utils::to_debug_string(idef);
        if(auto err = recovery::deserialize_into_provider(idef, *tables_, *tables_, false)) {
            LOG_LP(ERROR) << "Metadata recovery failed. Invalid metadata:" << *err;
            return err->status();
        }
    }
    return status::ok;
}

status database::setup_system_storage() {
    // if system table doesn't exist, create a kvs store, that will be recovered later in this start-up process
    std::vector<std::string> names{};
    auto stg = kvs_db_->get_storage(system_sequences_name);
    if(stg) {
        return status::ok;
    }
    auto provider = std::make_shared<yugawara::storage::configurable_provider>(); // just for serialize
    executor::add_builtin_tables(*provider);
    bool success = true;
    provider->each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const& i) {
        if(! success) return;
        std::string storage{};
        if(auto err = recovery::create_storage_option(*i, storage, utils::metadata_serializer_option{true})) {
            success = false;
            if(! VLOG_IS_ON(log_trace)) {  // avoid duplicate log entry with log_trace
                VLOG_LP(log_error) << "error_info:" << *err;
            }
            return;
        }
        ::sharksfin::StorageOptions options{};
        options.payload(std::move(storage));
        if(stg = kvs_db_->create_storage(id, options); ! stg) {
            success = false;
            return;
        }
    });
    if(! success) {
        return status::err_unknown;
    }
    return status::ok;
}

status database::recover_metadata() {
    std::vector<std::string> names{};
    if(auto res = kvs_db_->list_storages(names); res != status::ok) {
        return res;
    }
    if(std::find(names.cbegin(), names.cend(), legacy_system_sequences_name) != names.cend()) {
        // found deprecated system table - db should not start
        LOG(ERROR) << "database metadata version is too old to recover";
        return status::err_invalid_state;
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

scheduler::job_context::job_id_type database::do_create_transaction_async(
    create_transaction_callback on_completion,
    transaction_option const& option
) {
    return do_create_transaction_async(
        [on_completion = std::move(on_completion)](
            transaction_handle tx,
            status st,
            std::shared_ptr<api::error_info> info  //NOLINT(performance-unnecessary-value-param)
        ) {
            on_completion(tx, st, (info ? info->message() : ""));
        },
        option,
        {} //TODO
    );
}

scheduler::job_context::job_id_type database::do_create_transaction_async(  //NOLINT(readability-function-cognitive-complexity)
    create_transaction_callback_error_info on_completion,
    transaction_option const& option,
    request_info const& req_info
) {
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::begin);
    req->status(scheduler::request_detail_status::accepted);
    std::stringstream ss{};
    ss << option;
    req->transaction_option_spec(ss.str());
    log_request(*req);

    auto rctx = impl::create_request_context(
        *this,
        nullptr,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req_info,
        req
    );

    auto tx_pptr = std::make_shared<std::shared_ptr<transaction_context>>(); // tricky, but in order to pass shared_ptr& to lambda
    auto jobid = rctx->job()->id();
    auto t = scheduler::create_custom_task(rctx.get(),
        [this, rctx, option, tx_pptr, jobid]() {
            VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:starting"
                << " job_id:"
                << utils::hex(jobid)
                << " options:{" << rctx->job()->request()->transaction_option_spec() << "}";
            auto res = create_transaction_internal(*tx_pptr, option);
            if(res != status::ok) {
                // possibly option args are invalid
                if(res == status::err_invalid_argument) {
                    set_error(
                        *rctx,
                        error_code::target_not_found_exception,
                        string_builder{} << "Target specified in transaction option is not found. " << option  << string_builder::to_string,
                        res
                    );
                } else if(res == status::err_resource_limit_reached) {
                        set_error(
                            *rctx,
                            error_code::transaction_exceeded_limit_exception,
                            "The number of transactions exceeded the limit.",
                            res
                        );
                } else {
                    set_error(
                        *rctx,
                        error_code::sql_execution_exception,
                        string_builder{} << "creating transaction failed with error:" << res << string_builder::to_string,
                        res
                    );
                }
                scheduler::submit_teardown(*rctx);
                return model::task_result::complete;
            }
            VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:starting_end"
                << " job_id:"
                << utils::hex(jobid);
            if(! option.is_long() && ! option.readonly()) {
                scheduler::submit_teardown(*rctx);
                return model::task_result::complete;
            }

            auto cancel_enabled = utils::request_cancel_enabled(request_cancel_kind::transaction_begin_wait);
            auto canceled = std::make_shared<bool>();
            auto& ts = *rctx->scheduler();
            ts.schedule_conditional_task(
                scheduler::conditional_task{
                    rctx.get(),
                    [tx_pptr, rctx, canceled, cancel_enabled]() {
                        if(cancel_enabled) {
                            auto& res_src = rctx->req_info().response_source();
                            if(res_src && res_src->check_cancel()) {
                                *canceled = true;
                                return true;
                            }
                        }
                        return (*tx_pptr)->is_ready();
                    },
                    [rctx, canceled]() {
                        if(*canceled) {
                            cancel_request(*rctx);
                        }
                        scheduler::submit_teardown(*rctx, true);
                    },
                }
            );
            return model::task_result::complete;
        }, model::task_transaction_kind::none);  // create transaction is neither sticky nor in-transaction
    rctx->job()->callback([on_completion=std::move(on_completion), rctx, tx_pptr, jobid, req_info](){
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:started "
            << (*tx_pptr ? (*tx_pptr)->transaction_id() : "<tx id not available>")
            << " job_id:"
            << utils::hex(jobid);
        auto txidstr = *tx_pptr ? (*tx_pptr)->transaction_id() : "";
        if(rctx->status_code() == status::ok) {
            auto* tx = (*tx_pptr).get();  //NOLINT
            auto tx_type = utils::tx_type_from(*tx);
            tx->start_time(transaction_context::clock::now());
            external_log::tx_start(req_info, "", txidstr, tx_type, tx->label());
            tx->state(transaction_state_kind::active);
        }

        transaction_handle handle{};
        if (*tx_pptr) {
            handle = transaction_handle{
                (*tx_pptr)->surrogate_id(),
                (*tx_pptr)->option()->session_id()
            };
        }
        on_completion(
            handle,
            rctx->status_code(),
            api::impl::error_info::create(rctx->error_info())
        );
    });
    auto& ts = *rctx->scheduler();
    req->status(scheduler::request_detail_status::submitted);
    log_request(*req);
    ts.schedule_task(std::move(t));
    return jobid;
}

void database::print_diagnostic(std::ostream &os) {
    os << "/:jogasaki print diagnostics start" << std::endl;
    if(task_scheduler_) {
        task_scheduler_->print_diagnostic(os);
    }
    if(durability_manager_) {
        durability_manager_->print_diagnostic(os);
    }
    os << "/:jogasaki print diagnostics end" << std::endl;
}

std::string database::diagnostic_string() {
    std::stringstream ss{};
    print_diagnostic(ss);
    return ss.str();
}

jogasaki::status jogasaki::api::impl::database::list_tables(
    std::vector<std::string>& out
) {
    std::shared_ptr<error::error_info> err_info{};
    return list_tables(out, err_info);
}

jogasaki::status jogasaki::api::impl::database::list_tables(
    std::vector<std::string>& out,
    std::shared_ptr<error::error_info>& err_info
) {
    err_info = {};
    tables_->each_relation([&](std::string_view, std::shared_ptr<yugawara::storage::relation const> const& t) {
        if(utils::is_prefix(t->simple_name(), system_identifier_prefix)) {
            return;
        }
        out.emplace_back(t->simple_name());
    });
    return status::ok;
}

bool database::execute_load(
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::vector<std::string> files,
    callback on_completion
) {
    auto stmt = get_statement(prepared);
    if (stmt == nullptr) {
        auto m = string_builder{} << "prepared statement not found handle:" << prepared << string_builder::to_string;
        auto rc = status::err_invalid_argument;
        auto err = create_error_info(
            error_code::statement_not_found_exception,
            m,
            rc
        );
        on_completion(rc, std::move(err));
        return false;
    }
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::load);
    req->status(scheduler::request_detail_status::accepted);
    req->statement_text(stmt->body()->sql_text_shared());  //NOLINT
    log_request(*req);

    auto rctx = impl::create_request_context(
        *this,
        nullptr,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        {},
        req
    );

    auto ldr = executor::batch::batch_executor::create_batch_executor(
        std::move(files),
        executor::batch::batch_execution_info{
            std::move(stmt),
            std::move(parameters),
            this,
            [rctx]() {
                scheduler::submit_teardown(*rctx);
            }
        }
    );
    rctx->job()->callback([on_completion=std::move(on_completion), rctx, ldr](){  // callback is copy-based
        (void)ldr; // to keep ownership
        on_completion(ldr->state()->status_code(), ldr->state()->error_info());
    });

    auto& ts = *rctx->scheduler();
    req->status(scheduler::request_detail_status::submitted);
    log_request(*req);

    // non tx loader boostrap task
    ts.schedule_task(
        scheduler::create_custom_task(rctx.get(),
            [rctx, ldr]() {
                (void) rctx;
                ldr->bootstrap();
                return model::task_result::complete;
            }, model::task_transaction_kind::none)
    );
    return true;
}

std::shared_ptr<transaction_context> database::find_transaction(api::transaction_handle handle) {
    if (!handle.session_id().has_value()) {
        decltype(transactions_)::accessor acc{};
        if (transactions_.find(acc, handle)) {
            return acc->second;
        }
        return {};
    }
    decltype(transaction_stores_)::accessor acc{};
    if (transaction_stores_.find(acc, handle.session_id().value())) {
        return acc->second->lookup(handle);
    }
    return {};
}

std::shared_ptr<transaction_store> database::find_transaction_store(std::size_t session_id) {
    decltype(transaction_stores_)::accessor acc{};
    if (transaction_stores_.find(acc, session_id)) {
        return acc->second;
    }
    return {};
}

std::shared_ptr<statement_store> database::find_statement_store(std::size_t session_id) {
    decltype(statement_stores_)::accessor acc{};
    if (statement_stores_.find(acc, session_id)) {
        return acc->second;
    }
    return {};
}

bool database::remove_transaction_store(std::size_t session_id) {
    return transaction_stores_.erase(session_id);
}

bool database::remove_statement_store(std::size_t session_id) {
    return statement_stores_.erase(session_id);
}

std::shared_ptr<impl::prepared_statement> database::find_statement(api::statement_handle handle) {
    if (! handle.session_id().has_value()) {
        decltype(prepared_statements_)::accessor acc{};
        if (prepared_statements_.find(acc, handle)) {
            return acc->second;
        }
        return {};
    }
    decltype(statement_stores_)::accessor acc{};
    if (statement_stores_.find(acc, handle.session_id().value())) {
        return acc->second->lookup(handle);
    }
    return {};
}

std::size_t database::transaction_count() const {
    // this function is for testing purposes only and is not thread-safe
    std::size_t ret{};
    for (auto&& s : transaction_stores_) {
        ret += s.second->size();
    }
    ret += transactions_.size();
    return ret;
}

bool database::stop_requested() const noexcept {
    return stop_requested_;
}

utils::use_counter const& database::requests_inprocess() const noexcept {
    return requests_inprocess_;
}

std::shared_ptr<durability_manager> const& database::durable_manager() const noexcept {
    return durability_manager_;
}

}

namespace jogasaki::api {

std::shared_ptr<database> create_database(std::shared_ptr<class configuration> cfg) {
    return global::database_impl(std::make_shared<impl::database>(std::move(cfg)));
}

std::shared_ptr<database> create_database(std::shared_ptr<configuration> cfg, sharksfin::DatabaseHandle db) {
    return global::database_impl(std::make_shared<impl::database>(std::move(cfg), db));
}
} // namespace jogasaki::api::impl
