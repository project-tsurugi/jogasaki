/*
 * Copyright 2018-2026 Project Tsurugi.
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
#pragma once


#include <tateyama/grpc/distributed_query/service_adapter.h>

namespace jogasaki::api { class database; }

namespace jogasaki::dist {

/**
 * @brief execute a distributed-query process against this database and return encoded records.
 * @details Uses the existing process executor and owns no database. The database must outlive active calls.
 * @note Currently supports a read-only, single remote process with scan/find, filter/project, and one output.
 * @todo Extend supported plan shapes and output bindings together with the compiler and Portable IR contracts.
 */
class remote_process_handler final :
    public tateyama::grpc::distributed_query::remote_process_handler {
public:
    explicit remote_process_handler(api::database& database) noexcept;

    [[nodiscard]] tateyama::grpc::distributed_query::remote_process_result operator()(
        std::string_view payload) override;

    /** @details The cancellation predicate may be called by executor workers and must be thread-safe. */
    [[nodiscard]] tateyama::grpc::distributed_query::remote_process_result execute(
        std::string_view payload,
        cancellation_check const& is_cancelled) override;

    [[nodiscard]] tateyama::grpc::distributed_query::remote_process_result execute_records(
        std::string_view payload, cancellation_check const& is_cancelled, record_sink const& sink) override;

    [[nodiscard]] tateyama::grpc::distributed_query::remote_process_result execute_stream(
        std::string_view payload, cancellation_check const& is_cancelled, result_sink const& sink) override;

private:
    [[nodiscard]] tateyama::grpc::distributed_query::remote_process_result execute_impl(
        std::string_view payload, cancellation_check const& is_cancelled, result_sink const& sink,
        record_sink const& records = {});
    api::database* database_{};
};

} // namespace jogasaki::dist
