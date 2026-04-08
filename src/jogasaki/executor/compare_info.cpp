/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "compare_info.h"

#include <utility>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/assert.h>

namespace jogasaki::executor {

compare_info::compare_info(
    meta::record_meta const& left,
    meta::record_meta const& right,
    std::vector<ordering> orders
) :
    left_(std::addressof(left)),
    right_(std::addressof(right)),
    orders_(std::move(orders))
{
    assert_with_exception(left_->field_count() == right_->field_count(), left_->field_count(), right_->field_count());
    assert_with_exception(orders_.empty() || orders_.size() == left_->field_count(), orders_.size(), left_->field_count());
    for(std::size_t i=0, n = left_->field_count(); i < n; ++i) {
        (void)i;
        assert_with_exception(left_->at(i) == right_->at(i), left_->at(i), right_->at(i));
    }
}

compare_info::compare_info(
    meta::record_meta const& left,
    meta::record_meta const& right
) :
    left_(std::addressof(left)),
    right_(std::addressof(right))
{
    assert_with_exception(left_->field_count() == right_->field_count(), left_->field_count(), right_->field_count());
    for(std::size_t i=0, n = left_->field_count(); i < n; ++i) {
        (void)i;
        assert_with_exception(left_->at(i) == right_->at(i), left_->at(i), right_->at(i));
    }
}

compare_info::compare_info(const meta::record_meta& meta, std::vector<ordering> orders) :
    compare_info(meta, meta, std::move(orders))
{}

meta::record_meta const& compare_info::left() const noexcept {
    return *left_;
}

meta::record_meta const& compare_info::right() const noexcept {
    return *right_;
}

bool compare_info::opposite(std::size_t field_index) const noexcept {
    if (orders_.empty()) return false;
    return orders_[field_index] == ordering::descending;
}

}
