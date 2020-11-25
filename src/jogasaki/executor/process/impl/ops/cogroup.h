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

#include <takatori/relation/expression.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/utils/iterator_pair.h>
#include "operator_kind.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::sequence_view;

struct cache_align group_field {
    meta::field_type type_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
    bool is_key_{};
};

class group {
public:
    using iterator = data::iterable_record_store::iterator;
    using iterator_pair = utils::iterator_pair<iterator>;
    group(
        iterator_pair iterators,
        sequence_view<group_field> fields,
        accessor::record_ref key,
        std::size_t record_size
    ) noexcept :
        iterators_(iterators),
        fields_(fields),
        key_(key),
        record_size_(record_size)
    {}

    [[nodiscard]] iterator begin() const noexcept {
        return iterators_.first;
    }
    [[nodiscard]] iterator end() const noexcept {
        return iterators_.second;
    }
    [[nodiscard]] sequence_view<group_field> fields() const noexcept {
        return fields_;
    }
    [[nodiscard]] accessor::record_ref key() const noexcept {
        return key_;
    }

    [[nodiscard]] bool empty() const noexcept {
        return iterators_.first == iterators_.second;
    }

    [[nodiscard]] std::size_t record_size() const noexcept {
        return record_size_;
    }
private:
    iterator_pair iterators_;
    sequence_view<group_field> fields_{};
    accessor::record_ref key_{};
    std::size_t record_size_{};
};

class cogroup {
public:
    using iterator = data::iterable_record_store::iterator;
    using iterator_pair = utils::iterator_pair<iterator>;

    cogroup() = default;

    explicit cogroup(
        sequence_view<group> groups
    ) noexcept :
        groups_(groups)
    {}

    [[nodiscard]] sequence_view<group> groups() const noexcept {
        return groups_;
    }

private:
    sequence_view<group> groups_{};
};

}
