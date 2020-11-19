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
#include <memory>

#include "request_context.h"
#include "channel.h"

namespace jogasaki {

request_context::request_context() :
    channel_{std::make_shared<class channel>()},
    config_{std::make_shared<class configuration>()},
    compiler_context_{std::make_shared<plan::compiler_context>()}
{}

request_context::request_context(
    std::shared_ptr<class channel> ch,
    std::shared_ptr<class configuration> config,
    std::shared_ptr<plan::compiler_context> compiler_context,
    std::unique_ptr<memory::lifo_paged_memory_resource> request_resource,
    std::shared_ptr<kvs::database> database,
    std::shared_ptr<kvs::transaction> transaction,
    result_stores* stores,
    memory::paged_memory_resource* record_resource,
    memory::paged_memory_resource* varlen_resource
) :
    channel_(std::move(ch)),
    config_(std::move(config)),
    compiler_context_(std::move(compiler_context)),
    request_resource_(std::move(request_resource)),
    stores_(stores),
    database_(std::move(database)),
    transaction_(std::move(transaction)),
    result_record_resource_(record_resource),
    result_varlen_resource_(varlen_resource)
{}

std::shared_ptr<class channel> const& request_context::channel() const {
    return channel_;
}

std::shared_ptr<class configuration> const& request_context::configuration() const {
    return config_;
}

request_context::result_stores* request_context::stores() {
    return stores_;
}

std::shared_ptr<plan::compiler_context> const& request_context::compiler_context() const {
    return compiler_context_;
}

[[nodiscard]] std::shared_ptr<kvs::database> const& request_context::database() const {
    return database_;
}

[[nodiscard]] std::shared_ptr<kvs::transaction> const& request_context::transaction() const {
    return transaction_;
}

}

