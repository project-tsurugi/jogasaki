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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <optional>
#include <ostream>
#include <string_view>
#include <type_traits>
#include <unordered_map>

namespace jogasaki {

enum counter_kind : std::int32_t {
    undefined = 0,
    inserted,
    updated,
    merged,
    deleted,
    fetched,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(counter_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = counter_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::inserted: return "inserted"sv;
        case kind::updated: return "updated"sv;
        case kind::merged: return "merged"sv;
        case kind::deleted: return "deleted"sv;
        case kind::fetched: return "fetched"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, counter_kind value) {
    return out << to_string_view(value);
}

class request_execution_counter {
public:
    /**
     * @brief create new object
     */
    request_execution_counter() = default;

    /**
     * @brief destruct the object
     */
    ~request_execution_counter() = default;

    request_execution_counter(request_execution_counter const& other) = default;
    request_execution_counter& operator=(request_execution_counter const& other) = default;
    request_execution_counter(request_execution_counter&& other) noexcept = default;
    request_execution_counter& operator=(request_execution_counter&& other) noexcept = default;

    /**
     * @brief update the counter
     * @param arg to be added to the counter
     */
    void count(std::int64_t arg);

    /**
     * @brief accessor of the counter
     * @return the current value of the counter
     */
    [[nodiscard]] std::optional<std::int64_t> count() const noexcept;

    /**
     * @brief returns whether the counter has value
     */
    [[nodiscard]] bool has_value() const noexcept;

private:
    std::optional<std::int64_t> count_{};
};

/**
 * @brief statistics information on request execution
 */
class request_statistics {
public:
    using each_counter_consumer = std::function<void(counter_kind kind, request_execution_counter const&)>;

    using clock = std::chrono::system_clock;

    /**
     * @brief create new object
     */
    request_statistics() = default;

    /**
     * @brief destruct the object
     */
    ~request_statistics() = default;

    request_statistics(request_statistics const& other) = default;
    request_statistics& operator=(request_statistics const& other) = default;
    request_statistics(request_statistics&& other) noexcept = default;
    request_statistics& operator=(request_statistics&& other) noexcept = default;

    request_execution_counter& counter(counter_kind kind);

    void each_counter(each_counter_consumer consumer) const noexcept;

    void start_time(clock::time_point arg) noexcept;

    void end_time(clock::time_point arg) noexcept;

    template<class Duration>
    [[nodiscard]] Duration duration() const noexcept {
        return std::chrono::duration_cast<Duration>(end_time_ - start_time_);
    }

private:
    std::unordered_map<std::underlying_type_t<counter_kind>, request_execution_counter> entity_{};
    clock::time_point start_time_{};
    clock::time_point end_time_{};

};

}

