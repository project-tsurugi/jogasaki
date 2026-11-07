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
#include <jogasaki/data/any.h>
#include <jogasaki/lob/blob_locator.h>
#include <jogasaki/lob/clob_locator.h>

namespace jogasaki::data {
/** Immutable value plus its lifetime. All execution paths consume the same any representation. */
class value {
public:
    value() = default;
    ~value() = default;
    value(value const&) = default;
    value& operator=(value const&) = default;
    value(value&& other) noexcept
        : value_(std::exchange(other.value_, {})), owner_(std::move(other.owner_)) {}
    value& operator=(value&& other) noexcept {
        if (this != &other) {
            value_ = std::exchange(other.value_, {});
            owner_ = std::move(other.owner_);
        }
        return *this;
    }
    /** Copy variable-length bytes; scalar values need no allocation. */
    [[nodiscard]] static value copy(any source);
    [[nodiscard]] static value from_blob(lob::blob_locator locator);
    [[nodiscard]] static value from_clob(lob::clob_locator locator);
    [[nodiscard]] any view() const & noexcept { return value_; }
    [[nodiscard]] any view() const && = delete;
    [[nodiscard]] std::string_view bytes() const &;
    [[nodiscard]] std::string_view bytes() const && = delete;
    [[nodiscard]] bool empty() const noexcept { return value_.empty(); }
    [[nodiscard]] explicit operator bool() const noexcept { return static_cast<bool>(value_); }
private:
    value(any value, std::shared_ptr<void const> owner) : value_(value), owner_(std::move(owner)) {}
    any value_{};
    std::shared_ptr<void const> owner_{};
};
}
