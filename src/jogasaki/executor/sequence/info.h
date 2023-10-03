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

#include <jogasaki/common_types.h>

namespace jogasaki::executor::sequence {

/**
 * @brief sequence static information
 */
class info {
public:
    /**
     * @brief create empty object
     */
    info() = default;

    /**
     * @brief destruct the object
     */
    ~info() = default;

    info(info const& other) = default;
    info& operator=(info const& other) = default;
    info(info&& other) noexcept = default;
    info& operator=(info&& other) noexcept = default;

    /**
     * @brief create new object
     * @param sequence_definition_id the sequence definition id
     * @param sequence_id the sequence id
     * @param name the name of the sequence
     * @param initial_value initial value of the sequence
     * @param increment increment of the sequence
     * @param minimum_value minimum value of the sequence
     * @param maximum_value maximum value of the sequence
     * @param enable_cycle indicates whether cycle is enabled
     */
    info(
        sequence_definition_id sequence_definition_id,
        sequence_id sequence_id,
        std::string_view name,
        sequence_value initial_value,
        sequence_value increment,
        sequence_value minimum_value,
        sequence_value maximum_value,
        bool enable_cycle
    );

    /**
     * @brief create new object with default properties
     * @param sequence_definition_id the sequence definition id
     * @param sequence_id the sequence id
     * @param name the name of the sequence
     */
    info(
        sequence_definition_id sequence_definition_id,
        sequence_id sequence_id,
        std::string name
    );

    /**
     * @brief getter for definition id
     */
    [[nodiscard]] sequence_definition_id definition_id() const;

    /**
     * @brief getter for sequence id
     */
    [[nodiscard]] sequence_id id() const;

    /**
     * @brief getter for sequence name
     */
    [[nodiscard]] std::string_view name() const noexcept;

    /**
     * @brief getter for initial value
     */
    [[nodiscard]] sequence_value initial_value() const;

    /**
     * @brief getter for increment
     */
    [[nodiscard]] sequence_value increment() const;

    /**
     * @brief getter for min. value
     */
    [[nodiscard]] sequence_value minimum_value() const;

    /**
     * @brief getter for max. value
     */
    [[nodiscard]] sequence_value maximum_value() const;

    /**
     * @brief getter whether cycle is enabled
     */
    [[nodiscard]] bool cycle() const;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output stream
     */
    friend std::ostream& operator<<(std::ostream& out, info const& value) {
        return out <<
            "{"
            " sequence::info " << value.name_ <<
            " def_id: " << value.sequence_definition_id_ <<
            " id: " << value.sequence_id_ <<
            " initial: " << value.initial_value_ <<
            " incr: " << value.increment_ <<
            " min: " << value.minimum_value_ <<
            " max: " << value.maximum_value_ <<
            " cycle: " << value.enable_cycle_ <<
            "}";
    }

private:
    sequence_definition_id sequence_definition_id_{};
    sequence_id sequence_id_{};
    std::string name_{};
    sequence_value initial_value_{0};
    sequence_value increment_{1};
    sequence_value minimum_value_{1};
    sequence_value maximum_value_{std::numeric_limits<sequence_value>::max()};
    bool enable_cycle_{true};
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(info const& a, info const& b) noexcept {
    return a.definition_id() == b.definition_id() &&
        a.id() == b.id() &&
        a.increment() == b.increment() &&
        a.cycle() == b.cycle() &&
        a.name() == b.name() &&
        a.minimum_value() == b.minimum_value() &&
        a.maximum_value() == b.maximum_value() &&
        a.initial_value() == b.initial_value();
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(info const& a, info const& b) noexcept {
    return !(a == b);
}

}
