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
#pragma once

#include <cstddef>
#include <ostream>
#include <string_view>

namespace jogasaki::executor::process::impl {

/**
 * @brief kind of variable region that a region_id refers to
 */
enum class region_kind {
    undefined,
    basic_block,
    // TODO add more kind such as host variables
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(region_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = region_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::basic_block: return "basic_block"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, region_kind value) {
    return out << to_string_view(value);
}

/**
 * @brief identifies a variable region across potentially different region namespaces.
 * @details Wraps a region_kind and a raw region index to allow future extension
 *   (e.g. host-variable regions).  A default-constructed region_id is
 *   "undefined" and serves as a sentinel replacing std::optional<std::size_t>.
 *
 *   Implicit construction from std::size_t creates a basic_block region_id.
 */
class region_id {
public:
    /**
     * @brief create an undefined region_id
     */
    constexpr region_id() noexcept = default;

    /**
     * @brief create a basic_block region_id from a raw region index
     * @param index the zero-based region index
     */
    constexpr region_id(std::size_t index) noexcept :  // NOLINT(google-explicit-constructor,hicpp-explicit-conversions)
        kind_(region_kind::basic_block),
        index_(index)
    {}

    /**
     * @brief create a region_id with an explicit kind
     * @param kind the region kind
     * @param index the zero-based region index within that namespace
     */
    constexpr region_id(region_kind kind, std::size_t index) noexcept :
        kind_(kind),
        index_(index)
    {}

    /**
     * @brief return the kind of this region_id
     */
    [[nodiscard]] constexpr region_kind kind() const noexcept {
        return kind_;
    }

    /**
     * @brief return the raw region index
     * @details Only valid when kind() == region_kind::basic_block.
     *   Calling this on an undefined region_id is undefined behaviour;
     *   callers should always check operator bool() first.
     */
    [[nodiscard]] constexpr std::size_t index() const noexcept {
        return index_;
    }

    /**
     * @brief return true when this region_id refers to a concrete region (i.e. not undefined)
     */
    [[nodiscard]] constexpr operator bool() const noexcept {  //NOLINT(google-explicit-constructor,hicpp-explicit-conversions)
        return kind_ != region_kind::undefined;
    }

    constexpr bool operator==(region_id const& other) const noexcept {
        return kind_ == other.kind_ && index_ == other.index_;
    }

    constexpr bool operator!=(region_id const& other) const noexcept {
        return ! (*this == other);
    }

    /**
     * @brief appends a human-readable representation to the stream
     */
    friend inline std::ostream& operator<<(std::ostream& out, region_id const& id) {
        return out << to_string_view(id.kind_) << ":" << id.index_;
    }

private:
    region_kind kind_{region_kind::undefined};
    std::size_t index_{};
};

}  // namespace jogasaki::executor::process::impl
