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
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param id lob reference id
     * @param provider the provider that gives the lob data
     */
    lob_reference(lob_id_type id, lob_data_provider provider) :
        id_(id),
        provider_(provider)
    {}

    /**
     * @brief construct unresolved object
     * @param locator the locator of the lob data
     */
    explicit lob_reference(lob_locator const& locator) :
        locator_(std::addressof(locator))
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
     * @brief return whether the object is resolved
     * @return true if the object is resolved
     * @return false otherwise
     */
    [[nodiscard]] bool resolved() const noexcept {
        return locator_ == nullptr;
    }

    /**
     * @brief return locator of the lob data
     */
    [[nodiscard]] lob_locator const* locator() const noexcept {
        return locator_;
    }

    /**
     * @brief compare two lob object references
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(lob_reference const& a, lob_reference const& b) noexcept {
        if (! a.resolved() && ! b.resolved()) {
            return a.locator_ == b.locator_;
        }
        if (a.resolved() && b.resolved()) {
            return a.id_ == b.id_ && a.provider_ == b.provider_;
        }
        // one is resolved and the other is not
        return false;
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
        if (value.resolved()) {
            return out << "id:" << value.id_ << ",provider:" << value.provider_;
        }
        return out << *value.locator_;
    }

private:
    lob_id_type id_{};
    lob_data_provider provider_{};
    lob_locator const* locator_{};
};

static_assert(std::is_trivially_copyable_v<lob_reference>);
static_assert(std::is_trivially_destructible_v<lob_reference>);
static_assert(std::alignment_of_v<lob_reference> == 8);
static_assert(sizeof(lob_reference) == 24);

}  // namespace jogasaki::lob
