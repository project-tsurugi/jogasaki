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

#include <unordered_map>

#include <takatori/descriptor/variable.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief value location information
 */
class value_info {
public:
    /**
     * @brief create empty info
     */
    constexpr value_info() = default;

    /**
     * @brief create new object
     * @param value_offset offset of the value
     * @param nullity_offset nullity offset of the value
     */
    constexpr value_info(
        std::size_t value_offset,
        std::size_t nullity_offset
        ) noexcept : value_offset_(value_offset), nullity_offset_(nullity_offset)
    {}

    /**
     * @brief value offset getter for the value
     * @return value offset
     */
    [[nodiscard]] std::size_t value_offset() const noexcept {
        return value_offset_;
    }

    /**
     * @brief nullity offset getter for the value
     * @return nullity offset
     */
    [[nodiscard]] std::size_t nullity_offset() const noexcept {
        return nullity_offset_;
    }

private:
    std::size_t value_offset_{};
    std::size_t nullity_offset_{};
};

/**
 * @brief mapping of variables descriptors to value location information in the record reference
 * @details This map is associated with a single record metadata. Use in pair with the record_meta,
 * that holds fields offset/layout information of the record.
 */
class variable_value_map {
public:
    using variable = takatori::descriptor::variable;

    using entity_type = std::unordered_map<variable, value_info>;

    /**
     * @brief create new empty instance
     */
    constexpr variable_value_map() = default;

    /**
     * @brief create new instance from map
     */
    explicit variable_value_map(entity_type map) noexcept :
        map_(std::move(map))
    {}

    /**
     * @brief getter for value location info. for the given variable
     * @param var the variable descriptor
     * @return value_info for the variable
     */
    value_info const& at(variable const& var) const {
        return map_.at(var);
    }

private:
    entity_type map_{};
};

}


