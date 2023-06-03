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
#pragma once

namespace jogasaki::plan {

#include <string_view>
#include <ostream>
#include <type_traits>

enum class statement_work_level_kind : std::int32_t {
    //@brief level undefined
    undefined = -1,

    //@brief level zero - smallest workload
    zero = 0,

    //@brief statement contains statement::write
    simple_write = 10,

    //@brief statement contains statement::execute, values, find (only PK), emit, write
    key_operation = 20,

    //@brief statement contains find, filter (wo UDF), project (wo UDF)
    simple_crud = 30,

    //@brief statement contains forward, take_flat, offer
    simple_multirecord_operation = 40,

    //@brief statement contains group, join_find (wo UDF), take_group, take_cogroup, join_group (wo UDF)
    join = 50,

    //@brief statement contains aggregate (wo UDF, only built-in), aggregate_group (wo UDF, only built-in)
    aggregate = 60,

    //@brief level infinity - maximum workload
    infinity = 999,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(statement_work_level_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = statement_work_level_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::zero: return "zero"sv;
        case kind::simple_write: return "simple_write"sv;
        case kind::key_operation: return "key_operation"sv;
        case kind::simple_crud: return "simple_crud"sv;
        case kind::simple_multirecord_operation: return "simple_multirecord_operation"sv;
        case kind::join: return "join"sv;
        case kind::aggregate: return "aggregate"sv;
        case kind::infinity: return "infinity"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, statement_work_level_kind value) {
    return out << to_string_view(value);
}

/**
 * @brief statement work level
 */
class statement_work_level {
public:
    using underlying_type = std::underlying_type_t<statement_work_level_kind>;

    statement_work_level() = default;
    ~statement_work_level() = default;
    statement_work_level(statement_work_level const& other) = default;
    statement_work_level& operator=(statement_work_level const& other) = default;
    statement_work_level(statement_work_level&& other) noexcept = default;
    statement_work_level& operator=(statement_work_level&& other) noexcept = default;

    explicit statement_work_level(statement_work_level_kind level) :
            kind_(level)
    {}

    void set_minimum(statement_work_level_kind kind) noexcept {
        if(static_cast<underlying_type>(kind_) < static_cast<underlying_type>(kind)) {
            kind_ = kind;
        }
    }

    [[nodiscard]] statement_work_level_kind kind() const noexcept {
        return kind_;
    }

    [[nodiscard]] underlying_type value() const noexcept {
        return static_cast<underlying_type>(kind_);
    }
private:
    statement_work_level_kind kind_{statement_work_level_kind::undefined};
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(statement_work_level const& a, statement_work_level const& b) noexcept {
    return a.kind() == b.kind();
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(statement_work_level const& a, statement_work_level const& b) noexcept {
    return !(a == b);
}

}
