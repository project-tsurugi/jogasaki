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

#include <cstddef>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/constants.h>

namespace jogasaki::meta {

using takatori::util::maybe_shared_ptr;

/**
 * @brief represents group metadata holding record metadata on key and value parts
 */
class group_meta final {
public:
    /// @brief record meta type
    using record_meta_type = maybe_shared_ptr<record_meta>;

    /**
     * @brief construct empty object
     */
    group_meta();

    /**
     * @brief construct new object
     * @param key_meta key metadata
     * @param value_meta value metadata
     */
    group_meta(
            record_meta_type key_meta,
            record_meta_type value_meta
    );

    /**
     * @brief construct new object by copying given metadata
     * @param key_meta key metadata
     * @param value_meta value metadata
     */
    group_meta(
            record_meta const& key_meta,
            record_meta const& value_meta
    );

    /**
     * @brief key metadata accessor
     * @return record metadata of key part
     */
    [[nodiscard]] record_meta const& key() const noexcept;

    /**
     * @brief key metadata accessor
     * @return record metadata of key part
     */
    [[nodiscard]] record_meta_type const& key_shared() const noexcept;

    /**
     * @brief value metadata accessor
     * @return record metadata of value part
     */
    [[nodiscard]] record_meta const& value() const noexcept;

    /**
     * @brief value metadata accessor
     * @return record metadata of value part
     */
    [[nodiscard]] record_meta_type const& value_shared() const noexcept;

private:
    record_meta_type key_meta_{};
    record_meta_type value_meta_{};
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(group_meta const& a, group_meta const& b) noexcept {
    return a.key() == b.key() && a.value() == b.value();
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(group_meta const& a, group_meta const& b) noexcept {
    return !(a == b);
}

} // namespace

