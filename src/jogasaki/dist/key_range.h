/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <string_view>

namespace jogasaki::dist {

class key_range {
  public:
    /// @brief the key type.
    using key_type = std::string_view;

    /// @brief the endpoint type.
    using endpoint_type = enum { unspecified, inclusive, exclusive, prefix_inclusive };

    /**
     * @brief creates a whole range on index.
     */
    key_range() noexcept : begin_endpoint_(unspecified), end_endpoint_(unspecified){};

    /**
     * @brief creates a new range on index.
     * @param begin_key begin key of the range
     * @param begin_endpoint endpoint type of the begin key,
     * or unspecified if the range starts from head of the index
     * @param end_key end key of the range
     * @param end_endpoint endpoint type of the end key,
     * or unspecified if the range goes to tail of the index
     */
    key_range(key_type begin_key, endpoint_type begin_endpoint, key_type end_key,
        endpoint_type end_endpoint) noexcept
        : begin_key_(begin_key), begin_endpoint_(begin_endpoint), end_key_(end_key),
          end_endpoint_(end_endpoint){};

    /**
     * @brief returns the begin key of the range.
     * @return the begin key
     * @return don't care if begin_endpoint() returns unspecified
     */
    [[nodiscard]] key_type begin_key() const noexcept;

    /**
     * @brief returns the endpoint type of the begin key.
     * @return the endpoint type
     * @return unspecified if the range starts from head of the index
     */
    [[nodiscard]] endpoint_type begin_endpoint() const noexcept;

    /**
     * @brief returns the end key of the range.
     * @return the end key.
     * @return don't care if end_endpoint() returns unspecified
     */
    [[nodiscard]] key_type end_key() const noexcept;

    /**
     * @brief returns the endpoint type of the end key.
     * @return the endpoint type
     * @return unspecified if the range goes to tail of the index
     */
    [[nodiscard]] endpoint_type end_endpoint() const noexcept;

  private:
    key_type begin_key_;
    endpoint_type begin_endpoint_;
    key_type end_key_;
    endpoint_type end_endpoint_;
};

} // namespace jogasaki::dist
