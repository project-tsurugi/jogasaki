/*
 * Copyright 2018-2023 shark's fin project.
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
#include <vector>
#include <iostream>
#include <limits>
#include <string_view>

namespace jogasaki::kvs {

/**
 * @brief represents transaction options.
 */
class transaction_option final {
public:

    /**
     * @brief entity type for write preserves for the long transaction
     */
    using write_preserves_type = std::vector<std::string>;

    /**
     * @brief entity type for read areas for the long transaction
     */
    using read_areas_type = std::vector<std::string>;

    /**
     * @brief retries infinite times.
     */
    static inline constexpr std::size_t INF = std::numeric_limits<std::size_t>::max();

    /**
     * @brief transaction type
     */
    enum class transaction_type : std::int32_t {

        /**
         * @brief transaction is short period and governed by optimistic concurrency control
         */
        occ = 0x01,

        /**
         * @brief transaction is long transaction governed by batch concurrent control
         */
        ltx = 0x02,

        /**
         * @brief transaction is read only
         */
        read_only = 0x03,
    };

    /**
     * @brief construct object with default options
     */
    constexpr transaction_option() = default;

    /**
     * @brief construct new object
     */
    transaction_option(
        transaction_type type,
        write_preserves_type write_preserves,
        read_areas_type read_areas_inclusive,
        read_areas_type read_areas_exclusive
    ) noexcept :
        transaction_type_(type),
        write_preserves_(std::move(write_preserves)),
        read_areas_inclusive_(std::move(read_areas_inclusive)),
        read_areas_exclusive_(std::move(read_areas_exclusive))
    {}

    /**
     * @brief returns the maximum number of transaction retry attempts.
     * This is only enable if the following situations:
     * - user requested COMMIT operation in a transaction process, but transaction engine was failed, or
     * - user requested RETRY operation in a transaction process
     * On the other words, the transaction engine never retry if a user was requested either ROLLBACK or ERROR.
     * @return 0 if never retry
     * @return transaction_option::INF to try retry until fatal error was occurred
     * @return otherwise maximum retry count
     * @see TransactionOperation
     */
    [[nodiscard]] constexpr std::size_t retry_count() const noexcept {
        return retry_count_;
    }

    /**
     * @brief returns the transaction type.
     * @return the transaction type
     */
    [[nodiscard]] constexpr transaction_type type() const noexcept {
        return transaction_type_;
    }

    /**
     * @brief returns the write preserve objects.
     * @return the write preserve objects if set for the transaction
     * @return empty vector otherwise
     */
    [[nodiscard]] constexpr write_preserves_type const& write_preserves() const noexcept {
        return write_preserves_;
    }

    /**
     * @brief returns the inclusive read area objects.
     * @return the read area objects if set for the transaction
     * @return empty vector otherwise
     */
    [[nodiscard]] constexpr read_areas_type const& read_areas_inclusive() const noexcept {
        return read_areas_inclusive_;
    }

    /**
     * @brief returns the exclusive read area objects.
     * @return the read area objects if set for the transaction
     * @return empty vector otherwise
     */
    [[nodiscard]] constexpr read_areas_type const& read_areas_exclusive() const noexcept {
        return read_areas_exclusive_;
    }

    /**
     * @brief sets the maximum number of transaction retry attempts.
     * The default value is 0.
     * @param count the retry count; 0 - never, transaction_option::INF - infinity
     * @return this
     */
    constexpr inline transaction_option& retry_count(std::size_t count) noexcept {
        retry_count_ = count;
        return *this;
    }

    /**
     * @brief sets the transaction type.
     * The default value is TransactionType::SHORT.
     * @param type the transaction type to set
     * @return this
     */
    inline transaction_option& type(transaction_type type) noexcept {
        transaction_type_ = type;
        return *this;
    }

    /**
     * @brief sets the write preserve objects.
     * @param wp the write preserves to set
     * @return this
     */
    inline transaction_option& write_preserves(write_preserves_type write_preserves) noexcept {
        write_preserves_ = std::move(write_preserves);
        return *this;
    }

private:
    std::size_t retry_count_ { 0L };
    transaction_type transaction_type_ { transaction_type::occ };
    write_preserves_type write_preserves_{};
    read_areas_type read_areas_inclusive_{};
    read_areas_type read_areas_exclusive_{};
};

/**
 * @brief returns the label of the given enum value.
 * @param value the enum value
 * @return the corresponded label
 */
inline constexpr std::string_view to_string_view(transaction_option::transaction_type value) {
    switch (value) {
        case transaction_option::transaction_type::occ: return "occ";
        case transaction_option::transaction_type::ltx: return "ltx";
        case transaction_option::transaction_type::read_only: return "read_only";
    }
    std::abort();
}

/**
 * @brief appends enum label into the given stream.
 * @param out the target stream
 * @param value the source enum value
 * @return the target stream
 */
inline std::ostream& operator<<(std::ostream& out, transaction_option::transaction_type value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki::kvs
