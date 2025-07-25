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

#include <iosfwd>
#include <memory>
#include <deque>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::process::impl {

using takatori::util::maybe_shared_ptr;

/**
 * @brief variables storage
 */
class variable_table {
public:
    using lob_locator = std::variant<lob::blob_locator, lob::clob_locator>;

    /**
     * @brief construct empty instance
     */
    variable_table() = default;

    /**
     * @brief construct new instance
     */
    explicit variable_table(
        variable_table_info const& info
    );

    /**
     * @brief accessor to variable store
     */
    [[nodiscard]] data::small_record_store& store() const noexcept;

    /**
     * @brief accessor to metadata of variable store
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    /**
     * @brief accessor to variable table info
     */
    [[nodiscard]] variable_table_info const& info() const noexcept;

    /**
     * @brief return whether the object is non-empty
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief Support for debugging, callable in GDB: vt->dump()
     *
     * @param indent A string used for indentation in the output,
     * making it easier to read nested structures. Default is an empty string.
     */
    void dump(std::string const& indent = "") const noexcept;

    /**
     * @brief add lob locator to maintain the locator lifecycle for blob reference
     */
    template <class T>
    std::enable_if_t<std::is_same_v<lob::blob_locator, T> || std::is_same_v<lob::clob_locator, T>, T> const& add_lob_locator(T locator) {
        lob_locators_.emplace_back(std::in_place_type<T>, std::forward<T>(locator));
        return std::get<T>(lob_locators_.back());
    }

private:
    variable_table_info const* info_{};
    std::unique_ptr<data::small_record_store> store_{};
    std::deque<lob_locator> lob_locators_{};
};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, variable_table const& value);

}  // namespace jogasaki::executor::process::impl
