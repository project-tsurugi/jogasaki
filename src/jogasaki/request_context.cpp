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
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/api/transaction.h>
#include "error.h"

namespace jogasaki {

request_context::request_context() :
    config_(std::make_shared<class configuration>())
{}

request_context::request_context(
    std::shared_ptr<class configuration> config,
    std::shared_ptr<memory::lifo_paged_memory_resource> request_resource,
    std::shared_ptr<kvs::database> database,
    std::shared_ptr<kvs::transaction> transaction,
    executor::sequence::manager* sequence_manager,
    data::result_store* result
) :
    config_(std::move(config)),
    request_resource_(std::move(request_resource)),
    database_(std::move(database)),
    transaction_(std::move(transaction)),
    sequence_manager_(sequence_manager),
    result_(result)
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

[[nodiscard]] std::shared_ptr<kvs::transaction> const& request_context::transaction() const {
    return transaction_;
}

memory::lifo_paged_memory_resource* request_context::request_resource() const noexcept {
    return request_resource_.get();
}

void request_context::status_code(status val) noexcept {
    status s;
    do {
        s = status_code_.load();
        if (is_error(s)) {
            return;
        }
    } while (!status_code_.compare_exchange_strong(s, val));
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

}

