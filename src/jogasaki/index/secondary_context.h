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
#pragma once

#include <memory>
#include <vector>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>

namespace jogasaki::index {

/**
 * @brief secondary target context
 */
class secondary_context {
public:
    friend class secondary_target;

    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    secondary_context() = default;

    /**
     * @brief create new object
     */
    secondary_context(
        std::unique_ptr<kvs::storage> stg,
        request_context* rctx
    );

    /**
     * @brief accessor to the request context
     * @return request context
     */
    [[nodiscard]] request_context* req_context() const noexcept;

private:
    std::unique_ptr<kvs::storage> stg_{};
    data::aligned_buffer encoded_secondary_key_{};
    request_context* rctx_{};
};

}  // namespace jogasaki::index
