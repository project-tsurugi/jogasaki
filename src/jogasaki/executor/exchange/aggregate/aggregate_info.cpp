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
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::exchange::aggregate {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

using kind = meta::field_type_kind;

aggregate_info::aggregate_info(
    maybe_shared_ptr<meta::record_meta> record,
    std::vector<field_index_type> key_indices,
    std::vector<value_spec> mid_value_specs,
    std::vector<value_spec> post_value_specs
) :
    record_(std::move(record)),
    key_indices_(std::move(key_indices)),
    extracted_key_meta_(create_extracted_meta(key_indices_)),
    mid_(std::move(mid_value_specs), false, record_, key_indices_),
    post_(std::move(post_value_specs), true, record_, key_indices_)
{}

accessor::record_ref aggregate_info::extract_key(accessor::record_ref record) const noexcept {
    return accessor::record_ref(record.data(), record_->record_size());
}

const maybe_shared_ptr<meta::record_meta> &aggregate_info::record_meta() const noexcept {
    return record_;
}


sequence_view<const aggregate_info::field_index_type> aggregate_info::key_indices() const noexcept {
    return key_indices_;
}

std::shared_ptr<meta::record_meta> aggregate_info::create_extracted_meta(std::vector<std::size_t> const& indices) {
    auto num = indices.size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type  nullables(num);
    meta::record_meta::value_offset_table_type value_offset_table{};
    meta::record_meta::nullity_offset_table_type nullity_offset_table{};
    fields.reserve(num);
    value_offset_table.reserve(num);
    nullity_offset_table.reserve(num);
    for(std::size_t i=0; i < num; ++i) {
        auto ind = indices[i];
        fields.emplace_back(record_->at(ind));
        value_offset_table.emplace_back(record_->value_offset(ind));
        nullity_offset_table.emplace_back(record_->nullity_offset(ind));
        if (record_->nullable(ind)) {
            nullables.set(i);
        }
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables),
        std::move(value_offset_table),
        std::move(nullity_offset_table),
        record_->record_alignment(),
        record_->record_size()
    );
}

std::shared_ptr<meta::record_meta> aggregate_info::output_info::create_key_meta(
    bool post
) {
    auto num = key_indices_->size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type nullables(0);
    fields.reserve(num+1); // +1 for safety
    for(std::size_t i=0; i < num; ++i) {
        auto ind = key_indices_->at(i);
        fields.emplace_back(record_->at(ind));
        nullables.push_back(record_->nullable(ind));
    }
    std::size_t record_size = meta::record_meta::npos;
    if (! post) {
        fields.emplace_back(meta::field_type{enum_tag<kind::pointer>});
        nullables.push_back(true);
    } else {
        // post key doesn't have internal pointer field, but the record length is same as mid
        record_size = create_key_meta(false)->record_size();
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables),
        record_size
    );
}

std::shared_ptr<meta::record_meta> aggregate_info::output_info::create_value_meta(bool post) {
    (void)post;
    auto num = value_specs_.size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type nullables(num);
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

std::vector<std::vector<field_locator>> aggregate_info::output_info::create_source_field_locs(bool post) {
    (void)post;
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
        ret.emplace_back(std::move(arg));
    }
    return ret;
}

std::vector<field_locator> aggregate_info::output_info::create_target_field_locs(bool post) {
    (void)post;
    std::vector<field_locator> ret{};
    ret.reserve(value_count());
    auto& value_meta = group_->value();
    for(std::size_t i=0, n=value_specs_.size(); i < n; ++i) {
        auto& s = value_specs_[i];  // intermediate value specs
        ret.emplace_back(
            s.type(),
            value_meta.nullable(i),
            value_meta.value_offset(i),
            value_meta.nullity_offset(i)
        );
    }
    return ret;
}

field_locator const &aggregate_info::output_info::target_field_locator(std::size_t idx) const noexcept {
    return target_field_locs_[idx];
}

const maybe_shared_ptr<meta::record_meta> &aggregate_info::extracted_key_meta() const noexcept {
    return extracted_key_meta_;
}

accessor::record_ref aggregate_info::output_key(accessor::record_ref mid) const noexcept {
    return accessor::record_ref(mid.data(), post_.group_meta()->key().record_size());
}

accessor::record_ref aggregate_info::output_value(accessor::record_ref mid) const noexcept {

    return accessor::record_ref(mid.data(), post_.group_meta()->value().record_size());
}

}
