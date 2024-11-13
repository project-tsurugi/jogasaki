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

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/kvs/storage.h>

namespace jogasaki::executor::process::impl {

class bound {

  public:
    bound()                                  = default;
    bound(bound const& other)                = delete;
    bound& operator=(bound const& other)     = delete;
    bound(bound&& other) noexcept            = default;
    bound& operator=(bound&& other) noexcept = default;
    ~bound()                                 = default;
    bound(kvs::end_point_kind endpointkind, std::size_t len,
        std::unique_ptr<data::aligned_buffer> key)
        : endpointkind_(endpointkind), len_(len), key_(std::move(key)) {}
    kvs::end_point_kind endpointkind() { return endpointkind_; };
    std::unique_ptr<data::aligned_buffer> key() { return std::move(key_); };
    [[nodiscard]] std::string_view key() const noexcept;
    [[nodiscard]] kvs::end_point_kind endpointkind() const noexcept;
    /**
     * @brief Support for debugging, callable in GDB
     * @param out The output stream to which the buffer's internal state will be written.
     * @param indent The indentation level for formatting the output, default is 0.
     */
    void dump(std::ostream& out, int indent = 0) const noexcept;

  private:
    kvs::end_point_kind endpointkind_{};
    std::size_t len_{};
    std::unique_ptr<data::aligned_buffer> key_{};
};

} // namespace jogasaki::executor::process::impl