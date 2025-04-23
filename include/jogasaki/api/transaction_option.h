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

#include <string_view>
#include <memory>

#include <jogasaki/transaction_type_kind.h>

namespace jogasaki::api {

/**
 * @brief transaction option
 */
class transaction_option {
public:
    using scan_parallel_type = std::optional<std::uint32_t>;

    /**
     * @brief creates a new object (occ by default)
     */
    transaction_option() = default;

    /**
     * @brief destroys this object
     */
    ~transaction_option() = default;

    transaction_option(transaction_option const& other) = default;
    transaction_option& operator=(transaction_option const& other) = default;
    transaction_option(transaction_option&& other) noexcept = default;
    transaction_option& operator=(transaction_option&& other) noexcept = default;

    explicit transaction_option(
        transaction_type_kind type,
        std::vector<std::string> write_preserves = {},
        std::string_view label = {},
        std::vector<std::string> read_areas_inclusive = {},
        std::vector<std::string> read_areas_exclusive = {},
        bool modifies_definitions = false,
        scan_parallel_type rtx_scan_parallel = std::nullopt
    ) :
        type_(type),
        write_preserves_(std::move(write_preserves)),
        label_(label),
        read_areas_inclusive_(std::move(read_areas_inclusive)),
        read_areas_exclusive_(std::move(read_areas_exclusive)),
        modifies_definitions_(modifies_definitions),
        rtx_scan_parallel_(rtx_scan_parallel)
    {}

    //@deprecated use ctor with transaction_type_kind above
    explicit transaction_option(
        bool readonly,
        bool is_long = false,
        std::vector<std::string> write_preserves = {},
        std::string_view label = {},
        std::vector<std::string> read_areas_inclusive = {},
        std::vector<std::string> read_areas_exclusive = {},
        bool modifies_definitions = false
    ) :
        type_(readonly ? transaction_type_kind::rtx : (is_long ? transaction_type_kind::ltx : transaction_type_kind::occ)),
        write_preserves_(std::move(write_preserves)),
        label_(label),
        read_areas_inclusive_(std::move(read_areas_inclusive)),
        read_areas_exclusive_(std::move(read_areas_exclusive)),
        modifies_definitions_(modifies_definitions)
    {}

    transaction_option& readonly(bool arg) noexcept {
        if (arg) {
            type_ = transaction_type_kind::rtx;
        }
        return *this;
    }

    transaction_option& is_long(bool arg) noexcept {
        if (arg) {
            type_ = transaction_type_kind::ltx;
        }
        return *this;
    }

    [[nodiscard]] transaction_type_kind type() const noexcept {
        return type_;
    }

    [[nodiscard]] bool readonly() const noexcept {
        return type_ == transaction_type_kind::rtx;
    }

    [[nodiscard]] bool is_long() const noexcept {
        return type_ == transaction_type_kind::ltx;
    }

    [[nodiscard]] std::vector<std::string> const& write_preserves() const noexcept {
        return write_preserves_;
    }

    [[nodiscard]] std::string_view label() const noexcept {
        return label_;
    }

    [[nodiscard]] std::vector<std::string> const& read_areas_inclusive() const noexcept {
        return read_areas_inclusive_;
    }

    [[nodiscard]] std::vector<std::string> const& read_areas_exclusive() const noexcept {
        return read_areas_exclusive_;
    }

    transaction_option& modifies_definitions(bool arg) noexcept {
        modifies_definitions_ = arg;
        return *this;
    }

    [[nodiscard]] bool modifies_definitions() const noexcept {
        return modifies_definitions_;
    }

    transaction_option& rtx_scan_parallel(scan_parallel_type arg) noexcept {
        rtx_scan_parallel_ = arg;
        return *this;
    }

    [[nodiscard]] scan_parallel_type rtx_scan_parallel() const noexcept {
        return rtx_scan_parallel_;
    }

private:
    transaction_type_kind type_{transaction_type_kind::occ};
    std::vector<std::string> write_preserves_{};
    std::string label_{};
    std::vector<std::string> read_areas_inclusive_{};
    std::vector<std::string> read_areas_exclusive_{};
    bool modifies_definitions_ = false;
    std::optional<std::uint32_t> rtx_scan_parallel_{std::nullopt};
};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, transaction_option const& value) {
    out << "type:" << (value.is_long() ? "ltx" : (value.readonly() ? "rtx" : "occ")); //NOLINT
    out << " label:" << value.label();
    out << std::boolalpha << " modifies_definitions:" << value.modifies_definitions();
    out << " rtx_scan_parallel:";
    if (value.rtx_scan_parallel().has_value()) {
        out << value.rtx_scan_parallel().value();
    } else {
        out << "null";
    }
    if(! value.write_preserves().empty()) {
        out << " write_preserves:{";
        for (auto &&s: value.write_preserves()) {
            out << " ";
            out << s;
        }
        out << " }";
    }
    if(! value.read_areas_inclusive().empty()) {
        out << " read_areas_inclusive:{";
        for (auto &&s: value.read_areas_inclusive()) {
            out << " ";
            out << s;
        }
        out << " }";
    }
    if(! value.read_areas_exclusive().empty()) {
        out << " read_areas_exclusive:{";
        for (auto &&s: value.read_areas_exclusive()) {
            out << " ";
            out << s;
        }
        out << " }";
    }
    return out;
}
}
