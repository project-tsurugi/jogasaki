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

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include <jogasaki/meta/field_type.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using takatori::util::enum_tag_t;

class field_locator {
public:
    field_locator() = default;

    field_locator(
        meta::field_type const& type,
        bool nullable,
        std::size_t value_offset,
        std::size_t nullity_offset
    );

    [[nodiscard]] meta::field_type const& type() const noexcept;

    [[nodiscard]] bool nullable() const noexcept;

    [[nodiscard]] std::size_t value_offset() const noexcept;

    [[nodiscard]] std::size_t nullity_offset() const noexcept;
private:
    meta::field_type const* type_{};
    bool nullable_{};
    std::size_t value_offset_{};
    std::size_t nullity_offset_{};
};

}
