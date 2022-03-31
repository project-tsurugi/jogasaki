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
#include "request_context.h"

#include <memory>

#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/stealing_task_scheduler.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include "error.h"

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
    data::result_store* result,
    maybe_shared_ptr<api::data_channel> data_channel
) :
    config_(std::move(config)),
    request_resource_(std::move(request_resource)),
    database_(std::move(database)),
    transaction_(std::move(transaction)),
    sequence_manager_(sequence_manager),
    result_(result),
    data_channel_(std::move(data_channel))
{}

std::shared_ptr<class configuration> const& request_context::configuration() const {
    return config_;
}

[[nodiscard]] data::result_store* request_context::result() {
    return result_;
}

[[nodiscard]] std::shared_ptr<kvs::database> const& request_context::database() const {
    return database_;
}

[[nodiscard]] std::shared_ptr<transaction_context> const& request_context::transaction() const {
    return transaction_;
}

memory::lifo_paged_memory_resource* request_context::request_resource() const noexcept {
    return request_resource_.get();
}

bool request_context::status_code(status val) noexcept {
    status s;
    do {
        s = status_code_.load();
    } while (!status_code_.compare_exchange_strong(s, val));
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

void request_context::status_message(std::string_view val) noexcept {
    status_message_.assign(val);
}

std::string_view request_context::status_message() const noexcept {
    return status_message_;
}

maybe_shared_ptr<api::data_channel> const&  request_context::data_channel() const noexcept {
    return data_channel_;
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

void prepare_scheduler(request_context& rctx) {
    std::shared_ptr<scheduler::task_scheduler> sched{};
    if(rctx.configuration()->single_thread()) {
        sched = std::make_shared<scheduler::serial_task_scheduler>();
    } else {
        sched = std::make_shared<scheduler::stealing_task_scheduler>(
            scheduler::thread_params(rctx.configuration()));
    }
    rctx.scheduler(std::move(sched));

    rctx.stmt_scheduler(
        std::make_shared<scheduler::statement_scheduler>(
            rctx.configuration(),
            *rctx.scheduler()
        )
    );
}

}

