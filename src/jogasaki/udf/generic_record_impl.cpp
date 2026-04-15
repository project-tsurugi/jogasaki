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

#include "generic_record_impl.h"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "error_info.h"

namespace {

std::string value_type_to_string(plugin::udf::value_type const& v) {
    return std::visit(
        [](auto const& x) -> std::string {
            using T = std::decay_t<decltype(x)>;

            if constexpr (std::is_same_v<T, std::monostate>) {
                return "NULL";
            } else if constexpr (std::is_same_v<T, bool>) {
                return x ? "true" : "false";
            } else if constexpr (std::is_same_v<T, std::int32_t>) {
                return "int4(" + std::to_string(x) + ")";
            } else if constexpr (std::is_same_v<T, std::int64_t>) {
                return "int8(" + std::to_string(x) + ")";
            } else if constexpr (std::is_same_v<T, std::uint32_t>) {
                return "uint4(" + std::to_string(x) + ")";
            } else if constexpr (std::is_same_v<T, std::uint64_t>) {
                return "uint8(" + std::to_string(x) + ")";
            } else if constexpr (std::is_same_v<T, float>) {
                std::ostringstream os;
                os << "float4(" << x << ")";
                return os.str();
            } else if constexpr (std::is_same_v<T, double>) {
                std::ostringstream os;
                os << "float8(" << x << ")";
                return os.str();
            } else if constexpr (std::is_same_v<T, std::string>) {
                return "string(\"" + x + "\")";
            } else {
                static_assert(plugin::udf::always_false<T>::value, "unsupported value_type");
            }
        },
        v);
}

plugin::udf::runtime_type_kind to_runtime_type_kind(plugin::udf::value_type const& v) {
    if (std::holds_alternative<std::monostate>(v))
        return plugin::udf::runtime_type_kind::null_value;
    if (std::holds_alternative<bool>(v)) return plugin::udf::runtime_type_kind::boolean;
    if (std::holds_alternative<std::int32_t>(v)) return plugin::udf::runtime_type_kind::int4;
    if (std::holds_alternative<std::int64_t>(v)) return plugin::udf::runtime_type_kind::int8;
    if (std::holds_alternative<std::uint32_t>(v)) return plugin::udf::runtime_type_kind::uint4;
    if (std::holds_alternative<std::uint64_t>(v)) return plugin::udf::runtime_type_kind::uint8;
    if (std::holds_alternative<float>(v)) return plugin::udf::runtime_type_kind::float4;
    if (std::holds_alternative<double>(v)) return plugin::udf::runtime_type_kind::float8;
    return plugin::udf::runtime_type_kind::string;
}

} // namespace

namespace plugin::udf {

std::optional<error_info>& generic_record_impl::error() noexcept { return err_; }
std::optional<error_info> const& generic_record_impl::error() const noexcept { return err_; }

void generic_record_impl::reset() {
    values_.clear();
    err_ = std::nullopt;
}

void generic_record_impl::set_error(error_info const& status) {
    err_ = error_info(status.code(), std::string(status.message()));
}

void generic_record_impl::add_bool(bool v) { values_.emplace_back(v); }
void generic_record_impl::add_bool_null() { values_.emplace_back(std::monostate{}); }

void generic_record_impl::add_int4(std::int32_t v) { values_.emplace_back(v); }
void generic_record_impl::add_int4_null() { values_.emplace_back(std::monostate{}); }

void generic_record_impl::add_int8(std::int64_t v) { values_.emplace_back(v); }
void generic_record_impl::add_int8_null() { values_.emplace_back(std::monostate{}); }

void generic_record_impl::add_uint4(std::uint32_t v) { values_.emplace_back(v); }
void generic_record_impl::add_uint4_null() { values_.emplace_back(std::monostate{}); }

void generic_record_impl::add_uint8(std::uint64_t v) { values_.emplace_back(v); }
void generic_record_impl::add_uint8_null() { values_.emplace_back(std::monostate{}); }

void generic_record_impl::add_float(float v) { values_.emplace_back(v); }
void generic_record_impl::add_float_null() { values_.emplace_back(std::monostate{}); }

void generic_record_impl::add_double(double v) { values_.emplace_back(v); }
void generic_record_impl::add_double_null() { values_.emplace_back(std::monostate{}); }

void generic_record_impl::add_string(std::string v) { values_.emplace_back(std::move(v)); }
void generic_record_impl::add_string_null() { values_.emplace_back(std::monostate{}); }

std::unique_ptr<generic_record_cursor> generic_record_impl::cursor() const {
    return std::make_unique<generic_record_cursor_impl>(values_);
}

std::string generic_record_impl::debug_string() const {
    std::ostringstream os;
    os << "generic_record_impl{values=[";

    for (std::size_t i = 0; i < values_.size(); ++i) {
        if (i != 0) { os << ", "; }
        os << "#" << i << "=" << value_type_to_string(values_[i]);
    }

    os << "]";

    if (err_) {
        os << ", error={code=" << err_->code() << ", message=\"" << err_->message() << "\"}";
    } else {
        os << ", error=null";
    }

    os << "}";
    return os.str();
}

std::ostream& operator<<(std::ostream& os, generic_record_impl const& record) {
    return os << record.debug_string();
}

generic_record_cursor_impl::generic_record_cursor_impl(std::vector<value_type> const& values)
    : values_(values) {}

bool generic_record_cursor_impl::has_next() { return index_ < values_.size(); }

namespace {

template <typename T, typename Variant> std::optional<T> fetch_value_as(Variant const& v) {
    if (std::holds_alternative<std::monostate>(v)) { return std::nullopt; }
    if (auto p = std::get_if<T>(&v)) { return *p; }
    return std::nullopt;
}

template <typename T, typename ValueVector>
std::optional<T> fetch_and_advance(ValueVector const& values, std::size_t& index) {
    if (index >= values.size()) { return std::nullopt; }
    auto value = fetch_value_as<T>(values[index]);
    ++index; // Always consume exactly one logical slot, even for NULL.
    return value;
}

} // anonymous namespace

std::optional<bool> generic_record_cursor_impl::fetch_bool() {
    return fetch_and_advance<bool>(values_, index_);
}

std::optional<std::int32_t> generic_record_cursor_impl::fetch_int4() {
    return fetch_and_advance<std::int32_t>(values_, index_);
}

std::optional<std::int64_t> generic_record_cursor_impl::fetch_int8() {
    return fetch_and_advance<std::int64_t>(values_, index_);
}

std::optional<std::uint32_t> generic_record_cursor_impl::fetch_uint4() {
    return fetch_and_advance<std::uint32_t>(values_, index_);
}

std::optional<std::uint64_t> generic_record_cursor_impl::fetch_uint8() {
    return fetch_and_advance<std::uint64_t>(values_, index_);
}

std::optional<float> generic_record_cursor_impl::fetch_float() {
    return fetch_and_advance<float>(values_, index_);
}

std::optional<double> generic_record_cursor_impl::fetch_double() {
    return fetch_and_advance<double>(values_, index_);
}

std::optional<std::string> generic_record_cursor_impl::fetch_string() {
    return fetch_and_advance<std::string>(values_, index_);
}

plugin::udf::runtime_type_kind generic_record_cursor_impl::current_kind() const {
    if (index_ >= values_.size()) {
        throw std::out_of_range("generic_record_cursor: no current value");
    }
    return to_runtime_type_kind(values_[index_]);
}

bool generic_record_cursor_impl::current_is_null() const {
    if (index_ >= values_.size()) { return false; }
    return std::holds_alternative<std::monostate>(values_[index_]);
}

generic_record_stream_impl::generic_record_stream_impl() = default;
generic_record_stream_impl::~generic_record_stream_impl() { close(); }

generic_record_stream_impl::generic_record_stream_impl(
    generic_record_stream_impl&& other) noexcept {
    {
        std::lock_guard lk(other.mutex_);
        queue_ = std::move(other.queue_);
        closed_ = other.closed_;
        eos_ = other.eos_;
        other.closed_ = true;
        other.eos_ = true;
    }
    other.cv_.notify_all();
}

generic_record_stream_impl& generic_record_stream_impl::operator=(
    generic_record_stream_impl&& other) noexcept {
    if (this == &other) { return *this; }
    {
        std::scoped_lock lk(mutex_, other.mutex_);
        queue_ = std::move(other.queue_);
        closed_ = other.closed_;
        eos_ = other.eos_;
        other.closed_ = true;
        other.eos_ = true;
    }
    other.cv_.notify_all();
    return *this;
}

// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
void generic_record_impl::assign_from(generic_record_impl&& other) noexcept {
    values_ = std::move(other.values_);
    err_ = std::move(other.err_);
    other.reset();
}

void generic_record_stream_impl::push(std::unique_ptr<generic_record_impl> record) {
    {
        std::lock_guard lk(mutex_);
        if (closed_ || eos_) { return; }
        queue_.push(std::move(record));
    }
    cv_.notify_one();
}

void generic_record_stream_impl::end_of_stream() {
    {
        std::lock_guard lk(mutex_);
        eos_ = true;
    }
    cv_.notify_all();
}

void generic_record_stream_impl::close() {
    {
        std::lock_guard lk(mutex_);
        closed_ = true;
        eos_ = true;
        std::queue<std::unique_ptr<generic_record_impl>> empty;
        queue_.swap(empty);
    }
    cv_.notify_all();
}

generic_record_stream::status_type generic_record_stream_impl::extract_record_from_queue_unlocked(
    generic_record& record) {
    if (queue_.empty()) { return status_type::end_of_stream; }

    auto rec = std::move(queue_.front());
    queue_.pop();

    if (auto* impl = dynamic_cast<generic_record_impl*>(&record)) {
        impl->assign_from(std::move(*rec));
        return impl->error() ? status_type::error : status_type::ok;
    }
    return status_type::error;
}

generic_record_stream::status_type generic_record_stream_impl::try_next(generic_record& record) {
    std::lock_guard lk(mutex_);
    if (!queue_.empty()) { return extract_record_from_queue_unlocked(record); }
    if (eos_) { return status_type::end_of_stream; }
    return status_type::not_ready;
}

generic_record_stream::status_type generic_record_stream_impl::next(
    generic_record& record, std::optional<std::chrono::milliseconds> timeout) {
    std::unique_lock lk(mutex_);
    auto pred = [&] { return !queue_.empty() || eos_ || closed_; };

    if (timeout) {
        if (!cv_.wait_for(lk, *timeout, pred)) { return status_type::not_ready; }
    } else {
        cv_.wait(lk, pred);
    }

    if (!queue_.empty()) { return extract_record_from_queue_unlocked(record); }
    return status_type::end_of_stream;
}

} // namespace plugin::udf
