/*
 * Copyright 2018-2023 tsurugi project.
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

#include "transaction_type.h"
#include "transaction_priority.h"

namespace jogasaki::api::kvsservice::details {

using table_areas = std::vector<std::string>;

/**
 * @brief the transaction option.
 */
class transaction_option {
public:
    /**
     * @brief create new object
     */
    transaction_option() = default;

    /**
     * @brief create new object
     * @param type type of the transaction
     * @param write_preserves write preservations for long transactions
     */
    transaction_option(enum transaction_type type, table_areas write_preserves) noexcept;

    /**
     * @brief accessor to the transaction type
     */
    enum transaction_type type() const noexcept {
        return type_;
    };

    /**
     * @brief accessor to the write preservations
     */
    const table_areas &write_preserves() const noexcept {
        return write_preserves_;
    };

    void priority(enum transaction_priority priority) noexcept {
        priority_ = priority;
    }
    enum transaction_priority priority() const noexcept {
        return priority_;
    };

    void label(std::string_view label) noexcept {
        label_ = label;
    }
    const std::string &label() const noexcept {
        return label_;
    };


    void modifies_definitions(bool modify) noexcept {
        modifies_definitions_ = modify;
    }
    bool modifies_definitions() const noexcept {
        return modifies_definitions_;
    };

    void inclusive_read_areas(table_areas area) noexcept {
        inclusive_read_areas_ = std::move(area);
    }
    const table_areas &inclusive_read_areas() const noexcept {
        return inclusive_read_areas_;
    };

    void exclusive_read_areas(table_areas area) noexcept {
        exclusive_read_areas_ = std::move(area);
    }
    const table_areas &exclusive_read_areas() const noexcept {
        return exclusive_read_areas_;
    };

private:
    enum transaction_type type_ {};
    enum transaction_priority priority_ {};
    std::string label_ {};
    bool modifies_definitions_ {};
    table_areas write_preserves_ {};
    table_areas inclusive_read_areas_ {};
    table_areas exclusive_read_areas_ {};
};
}
