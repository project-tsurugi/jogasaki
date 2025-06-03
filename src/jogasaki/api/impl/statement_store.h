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
#pragma once

#include <memory>

#include <tbb/concurrent_hash_map.h>

#include <tateyama/api/server/session_element.h>

#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/impl/prepared_statement.h>

namespace jogasaki::api::impl {

class statement_store : public tateyama::api::server::session_element {
public:

    statement_store() = default;

    explicit statement_store(std::size_t session_id) noexcept;

    std::shared_ptr<prepared_statement> lookup(statement_handle handle);

    bool put(statement_handle handle,
             std::shared_ptr<prepared_statement> arg);

    bool remove(statement_handle handle);

    void dispose() override;

    [[nodiscard]] std::size_t size() const noexcept;

    [[nodiscard]] std::size_t session_id() const noexcept;

  private:
    std::size_t session_id_{};
    tbb::concurrent_hash_map<statement_handle, std::shared_ptr<prepared_statement>> statements_{};

};

}  // namespace jogasaki::api::impl
