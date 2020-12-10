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
#include "aggregate_info.h"

#include <vector>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/functions.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::exchange::aggregate {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

using kind = meta::field_type_kind;

aggregate_info::aggregate_info(maybe_shared_ptr<meta::record_meta> record, std::vector<field_index_type> key_indices,
    std::vector<value_spec> value_specs) :
    record_(std::move(record)),
    key_indices_(std::move(key_indices)),
    value_specs_(std::move(value_specs)),
    group_(std::make_shared<meta::group_meta>(create_key_meta(), create_value_meta())),
    args_(create_source_field_locs()),
    target_field_locs_(create_target_field_locs())
{}

accessor::record_ref aggregate_info::extract_key(accessor::record_ref record) const noexcept {
    return accessor::record_ref(record.data(), record_->record_size());
}

const maybe_shared_ptr<meta::record_meta> &aggregate_info::record_meta() const noexcept {
    return record_;
}

const maybe_shared_ptr<meta::record_meta> &aggregate_info::key_meta() const noexcept {
    return group_->key_shared();
}

const maybe_shared_ptr<meta::record_meta> &aggregate_info::value_meta() const noexcept {
    return group_->value_shared();
}

const maybe_shared_ptr<meta::group_meta> &aggregate_info::group_meta() const noexcept {
    return group_;
}

sequence_view<const aggregate_info::value_spec> aggregate_info::value_specs() const noexcept {
    return value_specs_;
}

sequence_view<const aggregate_info::field_index_type> aggregate_info::key_indices() const noexcept {
    return key_indices_;
}

sequence_view<const field_locator> aggregate_info::aggregator_args(std::size_t idx) const noexcept {
    return args_[idx];
}

std::shared_ptr<meta::record_meta> aggregate_info::create_key_meta() {
    auto num = key_indices_.size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type  nullables(num+1);
    fields.reserve(num+1);
    for(std::size_t i=0; i < num; ++i) {
        auto ind = key_indices_[i];
        fields.emplace_back(record_->at(ind));
        if (record_->nullable(ind)) {
            nullables.set(i);
        }
    }
    fields.emplace_back(meta::field_type{enum_tag<kind::pointer>});
    nullables.set(num);
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables)
    );
}

std::shared_ptr<meta::record_meta> aggregate_info::create_value_meta() {
    auto num = value_specs_.size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type  nullables(num);
    nullables.flip(); // assuming all values can be null
    fields.reserve(num);
    for(std::size_t i=0; i < num; ++i) {
        auto&& v = value_specs_[i];
        fields.emplace_back(v.type());
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables)
    );
}

std::size_t aggregate_info::value_count() const noexcept {
    return value_specs_.size();
}

std::vector<std::vector<field_locator>> aggregate_info::create_source_field_locs() {
    std::vector<std::vector<field_locator>> ret{};
    ret.reserve(value_specs_.size());
    for(auto&& vs : value_specs_) {
        std::vector<field_locator> arg{};
        arg.reserve(vs.argument_indices().size());
        for(auto i : vs.argument_indices()) {
            arg.emplace_back(
                record_->at(i),
                record_->nullable(i),
                record_->value_offset(i),
                record_->nullity_offset(i)
            );
        }
        args_.emplace_back(std::move(arg));
    }
    return ret;
}

std::vector<field_locator> aggregate_info::create_target_field_locs() {
    std::vector<field_locator> ret{};
    ret.reserve(value_count());
    for(std::size_t i=0, n=value_specs_.size(); i < n; ++i) {
        auto& s = value_specs_[i];  // intermediate value specs
        ret.emplace_back(
            s.type(),
            value_meta()->nullable(i),
            value_meta()->value_offset(i),
            value_meta()->nullity_offset(i)
        );
    }
    return ret;
}

field_locator const &aggregate_info::target_field_locator(std::size_t idx) const noexcept {
    return target_field_locs_[idx];
}

}
