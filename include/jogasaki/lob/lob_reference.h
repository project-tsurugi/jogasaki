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

#include <cstdint>
#include <type_traits>

#include <jogasaki/lob/lob_reference_kind.h>
#include <jogasaki/lob/lob_data_provider.h>
#include <jogasaki/lob/lob_id.h>
#include <jogasaki/lob/lob_locator.h>

namespace jogasaki::lob {

/**
 * @brief lob field data object
 * @details Trivially copyable immutable class holding lob reference.
 */
class lob_reference {
public:
    /**
     * @brief default constructor representing empty object
     */
    constexpr lob_reference() = default;

    /**
     * @brief construct `provided` object
     * @param locator the locator of the lob data
     */
    explicit lob_reference(lob_locator const& locator) :
        kind_(lob_reference_kind::provided),
        locator_(std::addressof(locator))
    {}

    /**
     * @brief construct `fetched` object
     * @param id the lob reference id fetched from datastore
     */
    explicit lob_reference(lob_id_type id) :
        kind_(lob_reference_kind::fetched),
        id_(id),
        provider_(lob_data_provider::datastore)
    {}

    /**
     * @brief construct `resolved` object
     * @param id lob reference id
     * @param provider the provider that gives the lob data
     */
    lob_reference(lob_id_type id, lob_data_provider provider, lob_reference_tag_type reference_tag) :
        kind_(lob_reference_kind::resolved),
        id_(id),
        provider_(provider),
        reference_tag_(reference_tag)
    {}

    /**
     * @brief return object id of the lob data
     */
    [[nodiscard]] lob_id_type object_id() const noexcept {
        return id_;
    }

    /**
     * @brief return provider of the lob data
     */
    [[nodiscard]] lob_data_provider provider() const noexcept {
        return provider_;
    }

    /**
     * @brief return reference tag of the lob data
     */
    [[nodiscard]] lob_reference_tag_type reference_tag() const noexcept {
        return reference_tag_;
    }

    /**
     * @brief return locator of the lob data
     */
    [[nodiscard]] lob_locator const* locator() const noexcept {
        return locator_;
    }

    /**
     * @brief return reference kind
     */
    [[nodiscard]] lob_reference_kind kind() const noexcept {
        return kind_;
    }

    /**
     * @brief compare two lob object references
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(lob_reference const& a, lob_reference const& b) noexcept {
        if (a.kind_ != b.kind_) {
            return false;
        }
        if (a.kind_ == lob_reference_kind::undefined) {
            return true;
        }
        if (a.kind_ == lob_reference_kind::provided) {
            return a.locator_ == b.locator_;
        }
        // fetched or resolved
        return a.id_ == b.id_ && a.provider_ == b.provider_;
    }

    /**
     * @brief compare two lob object references
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(lob_reference const& a, lob_reference const& b) noexcept {
        return ! (a == b);
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, lob_reference const& value) {
        out << "kind:" << value.kind_;
        if (value.kind_ == lob_reference_kind::undefined) {
            return out;
        }
        out << ",tag:" << value.reference_tag_;
        if (value.kind_ == lob_reference_kind::provided) {
            out << ",locator:";
            if(! value.locator_) {
                // normally locator should be non-null
                return out << "null";
            }
            return out << "{" << *value.locator_ << "}";
        }
        // fetched or resolved
        return out << ",id:" << value.id_ << ",provider:" << value.provider_;
    }

private:
    lob_reference_kind kind_{lob_reference_kind::undefined};
    lob_id_type id_{};
    lob_data_provider provider_{};
    lob_reference_tag_type reference_tag_{};
    lob_locator const* locator_{};
};

static_assert(std::is_trivially_copyable_v<lob_reference>);
static_assert(std::is_trivially_destructible_v<lob_reference>);
static_assert(std::alignment_of_v<lob_reference> == 8);
static_assert(sizeof(lob_reference) == 40); // this is not a fixed limit, but just to check the size is not unexpectedly large

}  // namespace jogasaki::lob
