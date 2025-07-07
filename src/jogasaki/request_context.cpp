/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "request_context.h"

#include <memory>
#include <ostream>
#include <utility>
#include <glog/logging.h>

#include <jogasaki/configuration.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/model/flow_repository.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/hybrid_task_scheduler.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/scheduler/stealing_task_scheduler.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/scheduler/thread_params.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki {

request_context::request_context() :
    config_(std::make_shared<class configuration>())
{}

request_context::request_context(
    std::shared_ptr<class configuration> config,
    std::shared_ptr<memory::lifo_paged_memory_resource> request_resource,
    std::shared_ptr<kvs::database> database,
    std::shared_ptr<transaction_context> transaction,
    executor::sequence::manager* sequence_manager,
    maybe_shared_ptr<executor::io::record_channel> record_channel
) :
    config_(std::move(config)),
    request_resource_(std::move(request_resource)),
    database_(std::move(database)),
    transaction_(std::move(transaction)),
    sequence_manager_(sequence_manager),
    record_channel_(std::move(record_channel))
{}

std::shared_ptr<class configuration> const& request_context::configuration() const {
    return config_;
}

[[nodiscard]] std::shared_ptr<kvs::database> const& request_context::database() const {
    return database_;
}

[[nodiscard]] std::shared_ptr<transaction_context> const& request_context::transaction() const {
    return transaction_;
}

void request_context::transaction(std::shared_ptr<transaction_context> arg) {
    transaction_ = std::move(arg);
}

memory::lifo_paged_memory_resource* request_context::request_resource() const noexcept {
    return request_resource_.get();
}

bool request_context::status_code(status val, std::string_view msg) noexcept {
    status s{};
    do {
        s = status_code_.load();
        if (s != status::ok) {
            if(val != status::err_inactive_transaction) {
                // Inactive tx occurs very frequently, so avoid logging here.
                VLOG_LP(log_error) << "Status code " << val << "(\"" << msg << "\")"
                                                                               " is reported subsequently following the original error " << s << ".";
            }
            return false;
        }
    } while (!status_code_.compare_exchange_strong(s, val));
    if(val != status::ok) {  // to ensure status::ok has no error msg
        status_message_.assign(msg);
    }
    return true;
}

status request_context::status_code() const noexcept {
    return status_code_.load();
}

maybe_shared_ptr<scheduler::job_context> const& request_context::job() const noexcept {
    return job_context_;
}

void request_context::job(maybe_shared_ptr<scheduler::job_context> arg) noexcept {
    job_context_ = std::move(arg);
}

executor::sequence::manager* request_context::sequence_manager() const noexcept {
    return sequence_manager_;
}

std::string_view request_context::status_message() const noexcept {
    return status_message_;
}

maybe_shared_ptr<executor::io::record_channel> const&  request_context::record_channel() const noexcept {
    return record_channel_;
}

void request_context::flows(maybe_shared_ptr<model::flow_repository> arg) noexcept {
    flows_ = std::move(arg);
}

maybe_shared_ptr<model::flow_repository> const& request_context::flows() const noexcept {
    return flows_;
}

void request_context::scheduler(maybe_shared_ptr<scheduler::task_scheduler> arg) noexcept {
    scheduler_ = std::move(arg);
}

maybe_shared_ptr<scheduler::task_scheduler> const& request_context::scheduler() const noexcept {
    return scheduler_;
}

void request_context::stmt_scheduler(maybe_shared_ptr<scheduler::statement_scheduler> arg) noexcept {
    statement_scheduler_ = std::move(arg);
}

maybe_shared_ptr<scheduler::statement_scheduler> const& request_context::stmt_scheduler() const noexcept {
    return statement_scheduler_;
}

void request_context::storage_provider(maybe_shared_ptr<yugawara::storage::configurable_provider> arg) noexcept {
    storage_provider_ = std::move(arg);
}

maybe_shared_ptr<yugawara::storage::configurable_provider> const& request_context::storage_provider() const noexcept {
    return storage_provider_;
}

void request_context::lightweight(bool arg) noexcept {
    lightweight_ = arg;
}

bool request_context::lightweight() const noexcept {
    return lightweight_;
}

bool request_context::error_info(std::shared_ptr<error::error_info> const& info) noexcept {
    std::shared_ptr<error::error_info> s{};
    s = std::atomic_load(std::addressof(error_info_));
    do {
        if (s && (*s)) {
            if(info->status() != status::err_inactive_transaction &&
                info->code() != error_code::inactive_transaction_exception) {
                // Inactive tx occurs very frequently, so avoid logging here.
                VLOG_LP(log_error) << "Error " << info->code() << "(\"" << info->message() << "\")"
                                                                                              " is reported subsequently following the original error " << s->code() << ".";
            }
            return false;
        }
    } while (! std::atomic_compare_exchange_strong(std::addressof(error_info_), std::addressof(s), info));
    return true;
}

std::shared_ptr<error::error_info> request_context::error_info() const noexcept {
    return std::atomic_load(std::addressof(error_info_));
}

std::shared_ptr<request_statistics> const& request_context::enable_stats() noexcept {
    if(! stats_) {
        stats_ = std::make_shared<request_statistics>();
    }
    return stats_;
}

std::shared_ptr<request_statistics> const& request_context::stats() const noexcept {
    return stats_;
}

void prepare_scheduler(request_context& rctx) {
    std::shared_ptr<scheduler::task_scheduler> sched{};
    if(rctx.configuration()->single_thread()) {
        sched = std::make_shared<scheduler::serial_task_scheduler>();
    } else {
        if(rctx.configuration()->enable_hybrid_scheduler()) {
            sched = std::make_shared<scheduler::hybrid_task_scheduler>(
                scheduler::thread_params(rctx.configuration()));
        } else {
            sched = std::make_shared<scheduler::stealing_task_scheduler>(
                scheduler::thread_params(rctx.configuration()));
        }
    }
    rctx.scheduler(std::move(sched));

    rctx.stmt_scheduler(
        std::make_shared<scheduler::statement_scheduler>(
            rctx.configuration(),
            *rctx.scheduler()
        )
    );
}

request_info const& request_context::req_info() const noexcept {
    return req_info_;
}

void request_context::req_info(request_info req_info) noexcept {
    req_info_ = std::move(req_info);
}

std::shared_ptr<commit_context> const& request_context::commit_ctx() const noexcept {
    return commit_ctx_;
}

void request_context::commit_ctx(std::shared_ptr<commit_context> arg) noexcept {
    commit_ctx_ = std::move(arg);
}

std::unique_ptr<storage::shared_lock> const& request_context::storage_lock() const noexcept {
    return storage_lock_;
}

void request_context::storage_lock(std::unique_ptr<storage::shared_lock> arg) noexcept {
    storage_lock_ = std::move(arg);
}

}  // namespace jogasaki
