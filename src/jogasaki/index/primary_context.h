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

#include <cstddef>
#include <memory>
#include <string_view>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/request_context.h>

namespace jogasaki::index {

using takatori::util::maybe_shared_ptr;

/**
 * @brief primary target context
 */
class primary_context {
public:

    friend class primary_target;
    using memory_resource = memory::lifo_paged_memory_resource;

    /**
     * @brief create empty object
     */
    primary_context() = default;

    /**
     * @brief create new object
     */
    primary_context(
        std::unique_ptr<kvs::storage> stg,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta,
        request_context* rctx
    );

    /**
     * @brief accessor to the encoded key
     * @return the encoded key
     * @pre primary_target::encode_put() or find_record_and_extract() is called with this object beforehand
     */
    [[nodiscard]] std::string_view encoded_key() const noexcept;

    /**
     * @brief accessor to the extracted key store
     * @return the encoded key
     * @pre primary_target::encode_find_remove() is called with this object beforehand
     */
    [[nodiscard]] accessor::record_ref extracted_key() const noexcept;

    /**
     * @brief accessor to the extracted value store
     * @return the encoded key
     * @pre primary_target::encode_find_remove() is called with this object beforehand
     */
    [[nodiscard]] accessor::record_ref extracted_value() const noexcept;

    /**
     * @brief accessor to the request context
     * @return request context
     */
    [[nodiscard]] request_context* req_context() const noexcept;

private:

    std::unique_ptr<kvs::storage> stg_{};
    data::aligned_buffer key_buf_{};  // internal buffer used from primary_target
    data::aligned_buffer value_buf_{};  // internal buffer used from primary_target
    data::small_record_store extracted_key_store_{};
    data::small_record_store extracted_value_store_{};
    std::size_t key_len_{};
    request_context* rctx_{};
};

}  // namespace jogasaki::index
