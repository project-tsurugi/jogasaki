/*
 * Copyright 2018-2022 tsurugi project.
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
#include "transaction_context.h"

#include <atomic>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/configuration.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>

namespace jogasaki {

using takatori::util::maybe_shared_ptr;

transaction_context::transaction_context(std::shared_ptr<kvs::transaction> transaction) :
    transaction_(std::move(transaction)),
    id_(id_source_++)
{}

transaction_context::operator kvs::transaction&() const noexcept {  //NOLINT
    return *transaction_;
}

std::shared_ptr<kvs::transaction> const& transaction_context::object() const {
    return transaction_;
}

std::size_t transaction_context::id() const noexcept {
    return id_;
}

transaction_context::operator bool() const noexcept {
    return transaction_ != nullptr;
}

status transaction_context::commit(bool async) {
    return transaction_->commit(async);
}

status transaction_context::wait_for_commit(std::size_t timeout_ns) {
    return transaction_->wait_for_commit(timeout_ns);
}

status transaction_context::abort() {
    return transaction_->abort();
}

sharksfin::TransactionControlHandle transaction_context::control_handle() const noexcept {
    return transaction_->control_handle();
}

sharksfin::TransactionHandle transaction_context::handle() noexcept {
    return transaction_->handle();
}

kvs::database* transaction_context::database() const noexcept {
    return transaction_->database();
}

std::shared_ptr<transaction_context> wrap(std::unique_ptr<kvs::transaction> arg) noexcept {
    return std::make_shared<transaction_context>(std::shared_ptr<kvs::transaction>{std::move(arg)});
}
}

