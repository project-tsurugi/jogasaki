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

#include <vector>
#include <set>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

class field_locator {
public:
    field_locator() = default;

    field_locator(
        meta::field_type const& type,
        bool nullable,
        std::size_t value_offset,
        std::size_t nullity_offset
    ) :
        type_(std::addressof(type)),
        nullable_(nullable),
        value_offset_(value_offset),
        nullity_offset_(nullity_offset)
    {}

    [[nodiscard]] meta::field_type const& type() const noexcept {
        return *type_;
    }

    [[nodiscard]] bool nullable() const noexcept {
        return nullable_;
    }

    [[nodiscard]] std::size_t value_offset() const noexcept {
        return value_offset_;
    }

    [[nodiscard]] std::size_t nullity_offset() const noexcept {
        return nullity_offset_;
    }
private:
    meta::field_type const* type_{};
    bool nullable_{};
    std::size_t value_offset_{};
    std::size_t nullity_offset_{};
};

using aggregator_type = std::function<void (
    accessor::record_ref,
    field_locator const&,
    bool,
    accessor::record_ref,
    sequence_view<field_locator const>
)>;

}
