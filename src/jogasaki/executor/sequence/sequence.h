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

#include <atomic>
#include <unordered_set>

#include <takatori/util/either.h>

#include <jogasaki/common_types.h>
#include <jogasaki/executor/sequence/info.h>
#include <jogasaki/kvs/transaction.h>

namespace jogasaki::executor::sequence {

using takatori::util::either;

class manager;

enum class sequence_error : std::size_t {
    unknown = 0,
    out_of_upper_bound,
    out_of_lower_bound,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(sequence_error value) noexcept {
    using namespace std::string_view_literals;
    using kind = sequence_error;
    switch (value) {
        case kind::unknown: return "unknown"sv;
        case kind::out_of_upper_bound: return "out_of_upper_bound"sv;
        case kind::out_of_lower_bound: return "out_of_lower_bound"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, sequence_error value) {
    return out << to_string_view(value);
}

/**
 * @brief in-memory sequence object
 * @details represent thread-safe updatable sequence object managed in-memory
 */
class sequence {
public:
    /**
     * @brief create empty object
     */
    sequence() = default;

    /**
     * @brief destruct the object
     */
    ~sequence() = default;

    sequence(sequence const& other) = delete;
    sequence& operator=(sequence const& other) = delete;
    sequence(sequence&& other) noexcept = delete;
    sequence& operator=(sequence&& other) noexcept = delete;

    /**
     * @brief create new object
     * @param info the sequence static information
     * @param parent the owner of the sequence
     * @param version the current version of the sequence
     * @param value the current value of the sequence
     */
    sequence(
        class info const& info,
        manager& parent,
        sequence_version version,
        sequence_value value
    );

    /**
     * @brief accessor to the current version/value of the sequence
     */
    [[nodiscard]] sequence_versioned_value get() const noexcept;

    /**
     * @brief get next sequence value
     * @param tx the transaction associated with this sequence value update
     * @return the next sequence value or an error
     */
    [[nodiscard]] either<sequence_error, sequence_value> next(kvs::transaction& tx);

    /**
     * @brief accessor to the sequence info
     */
    [[nodiscard]] class info const& info() const noexcept;

private:
    class info const* info_{};
    manager* parent_{};
    std::atomic<sequence_versioned_value> body_{};

};

/**
 * @brief equality comparison operator
 */
inline bool operator==(sequence const& a, sequence const& b) noexcept {
    return a.info() == b.info() &&
        a.get() == b.get();
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(sequence const& a, sequence const& b) noexcept {
    return !(a == b);
}

}  // namespace jogasaki::executor::sequence
