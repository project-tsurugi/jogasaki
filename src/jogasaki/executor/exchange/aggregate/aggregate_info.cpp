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
#include <jogasaki/executor/function/aggregator_info.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::exchange::aggregate {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

using kind = meta::field_type_kind;

using function::aggregate_function_info;
using function::aggregator_info;
using function::field_locator;

aggregate_info::aggregate_info(
    maybe_shared_ptr<meta::record_meta> record,
    std::vector<field_index_type> key_indices,
    std::vector<value_spec> value_specs
) :
    record_(std::move(record)),
    key_indices_(std::move(key_indices)),
    extracted_key_meta_(create_extracted_meta(key_indices_)),
    pre_(create_output(value_specs, output_kind::pre, record_, record_, key_indices_)),
    mid_(create_output(value_specs, output_kind::mid, pre_.group_meta()->value_shared(), record_, key_indices_)),
    post_(create_output(value_specs, output_kind::post, mid_.group_meta()->value_shared(), record_, key_indices_))
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
    output_kind kind
) {
    // only post output differs in that it doesn't have internal pointer field
    auto post = kind == output_kind::post;
    auto num = key_indices_->size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type nullables(0);
    fields.reserve(num+1); // +1 for safety
    for(std::size_t i=0; i < num; ++i) {
        auto ind = key_indices_->at(i);
        fields.emplace_back(pre_input_meta_->at(ind));
        nullables.push_back(pre_input_meta_->nullable(ind));
    }
    std::size_t record_size = meta::record_meta::npos;
    if (! post) {
        fields.emplace_back(meta::field_type{enum_tag<kind::pointer>});
        nullables.push_back(true);
    } else {
        // post key doesn't have internal pointer field, but the record length is same as mid
        record_size = create_key_meta(output_kind::mid)->record_size();
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables),
        record_size
    );
}

std::shared_ptr<meta::record_meta> aggregate_info::output_info::create_value_meta(
    output_kind kind
) {
    (void)kind;
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

std::vector<std::vector<field_locator>> aggregate_info::output_info::create_source_field_locs(
    output_kind kind
) {
    (void)kind;
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

std::vector<field_locator> aggregate_info::output_info::create_target_field_locs(
    output_kind kind
) {
    (void)kind;
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

field_locator const &aggregate_info::output_info::target_field_locator(std::size_t aggregator_index) const noexcept {
    return target_field_locs_[aggregator_index];
}

aggregate_info::output_info::output_info(std::vector<value_spec> value_specs, aggregate_info::output_kind kind,
    const maybe_shared_ptr<meta::record_meta> &pre_input_meta, const maybe_shared_ptr<meta::record_meta> &record,
    const std::vector<field_index_type> &key_indices) :
    value_specs_(std::move(value_specs)),
    kind_(kind),
    pre_input_meta_(std::move(pre_input_meta)),
    record_(std::move(record)),
    key_indices_(std::addressof(key_indices)),
    group_(std::make_shared<meta::group_meta>(create_key_meta(kind_), create_value_meta(kind_))),
    args_(create_source_field_locs(kind_)),
    target_field_locs_(create_target_field_locs(kind_))
{}

const maybe_shared_ptr<meta::group_meta> &aggregate_info::output_info::group_meta() const noexcept {
    return group_;
}

sequence_view<const aggregate_info::value_spec> aggregate_info::output_info::value_specs() const noexcept {
    return value_specs_;
}

std::size_t aggregate_info::output_info::value_count() const noexcept {
    return value_specs_.size();
}

sequence_view<const field_locator>
aggregate_info::output_info::aggregator_args(std::size_t aggregator_index) const noexcept {
    return args_[aggregator_index];
}

const maybe_shared_ptr<meta::record_meta> &aggregate_info::extracted_key_meta() const noexcept {
    return extracted_key_meta_;
}

accessor::record_ref aggregate_info::output_key(accessor::record_ref mid) const noexcept {
    return accessor::record_ref(mid.data(), post_.group_meta()->key().record_size());
}

accessor::record_ref aggregate_info::output_value(accessor::record_ref mid) const noexcept {
    // TODO is this needed?
    return accessor::record_ref(mid.data(), post_.group_meta()->value().record_size());
}

std::vector<meta::field_type> types(meta::record_meta const& meta, std::vector<std::size_t> indices) {
    std::vector<meta::field_type> ret{};
    ret.reserve(indices.size());
    for(auto i : indices) {
        ret.emplace_back(meta.at(i));
    }
    return ret;
}

aggregate_info::output_info aggregate_info::create_output(
    std::vector<value_spec> const& value_specs,
    output_kind kind,
    maybe_shared_ptr<meta::record_meta> record,
    maybe_shared_ptr<meta::record_meta> pre_input_meta,
    std::vector<field_index_type> const& key_indices
) {
    std::vector<value_spec> aggregator_specs{};
    for(std::size_t value_index = 0; value_index < value_specs.size(); ++value_index) {
        auto& vs = value_specs[value_index];
        auto& info = vs.function_info();
        std::size_t agg_index = 0;
        switch(kind) {
            case output_kind::pre: {
                auto aggs = info.pre();
                std::vector<size_t> arg_indices{vs.argument_indices().begin(), vs.argument_indices().end()};
                auto ts = types(*record, arg_indices);
                auto seq = info.internal_field_types(ts);
                BOOST_ASSERT(seq.size() == aggs.size());  //NOLINT
                for(std::size_t i=0, n=aggs.size(); i < n; ++i) {
                    aggregator_specs.emplace_back(
                        aggs[i],
                        arg_indices,
                        seq[i]
                    );
                }
                break;
            }
            case output_kind::mid: {
                auto aggs = info.mid();
                std::vector<size_t> indices{agg_index};
                for(std::size_t i=0, n=aggs.size(); i < n; ++i) {
                    aggregator_specs.emplace_back(
                        aggs[i],
                        indices,
                        record->at(agg_index)
                    );
                    ++agg_index;
                }
                break;
            }
            case output_kind::post: {
                auto aggs = info.post();
                BOOST_ASSERT(aggs.size() == 1);
                std::vector<size_t> indices{};
                for(std::size_t i=0, n=aggs[0].arg_count(); i < n; ++i) {
                    indices.emplace_back(agg_index);
                    ++agg_index;
                }
                aggregator_specs.emplace_back(
                    aggs[0],
                    indices,
                    vs.type()
                );
                break;
            }
        }
    }
    return output_info{
        std::move(aggregator_specs),
        kind,
        pre_input_meta,
        record,
        key_indices,
    };
}

aggregate_info::value_spec::value_spec(const aggregate_function_info &function_info,
    std::vector<std::size_t> argument_indices, meta::field_type type) noexcept:
    function_info_(std::addressof(function_info)),
    argument_indices_(std::move(argument_indices)),
    type_(std::move(type))
{}

aggregate_info::value_spec::value_spec(class aggregator_info const& aggregator_info,
    std::vector<std::size_t> argument_indices, meta::field_type type) noexcept:
    aggregator_info_(std::addressof(aggregator_info)),
    argument_indices_(std::move(argument_indices)),
    type_(std::move(type))
{}

aggregate_function_info const &aggregate_info::value_spec::function_info() const noexcept {
    return *function_info_;
}

class aggregator_info const &aggregate_info::value_spec::aggregator_info() const noexcept {
    return *aggregator_info_;
}

sequence_view<const std::size_t> aggregate_info::value_spec::argument_indices() const noexcept {
    return argument_indices_;
}

meta::field_type const &aggregate_info::value_spec::type() const noexcept {
    return type_;
}
}
