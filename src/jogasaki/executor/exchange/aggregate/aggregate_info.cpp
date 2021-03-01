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
#include <takatori/util/enum_tag.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/function/incremental/field_locator.h>
#include <jogasaki/executor/function/incremental/aggregator_info.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::executor::exchange::aggregate {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::enum_tag;

using kind = meta::field_type_kind;

using function::incremental::aggregate_function_info;
using function::incremental::aggregator_info;
using function::incremental::field_locator;

aggregate_info::aggregate_info(
    maybe_shared_ptr<meta::record_meta> record,
    std::vector<field_index_type> key_indices,
    std::vector<value_spec> const& value_specs,
    bool generate_record_on_empty
) :
    record_(std::move(record)),
    key_indices_(std::move(key_indices)),
    extracted_key_meta_(create_extracted_meta(key_indices_, record_)),
    pre_(create_output(output_kind::pre, value_specs, record_, record_, key_indices_)),
    mid_(create_output(output_kind::mid, value_specs, pre_.group_meta()->value_shared(), record_, key_indices_)),
    post_(create_output(output_kind::post, value_specs, mid_.group_meta()->value_shared(), record_, key_indices_)),
    generate_record_on_empty_(generate_record_on_empty)
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

std::shared_ptr<meta::record_meta> aggregate_info::create_extracted_meta(
    std::vector<std::size_t> const& indices,
    maybe_shared_ptr<meta::record_meta> const& aggregate_input
) {
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
        fields.emplace_back(aggregate_input->at(ind));
        value_offset_table.emplace_back(aggregate_input->value_offset(ind));
        nullity_offset_table.emplace_back(aggregate_input->nullity_offset(ind));
        if (aggregate_input->nullable(ind)) {
            nullables.set(i);
        }
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables),
        std::move(value_offset_table),
        std::move(nullity_offset_table),
        aggregate_input->record_alignment(),
        aggregate_input->record_size()
    );
}

std::shared_ptr<meta::record_meta> aggregate_info::output_info::create_key_meta(
    output_kind kind,
    std::vector<field_index_type> const& key_indices,
    maybe_shared_ptr<meta::record_meta> const& aggregate_input
) {
    // only post output differs in that it doesn't have internal pointer field
    auto post = kind == output_kind::post;
    auto num = key_indices.size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type nullables(0);
    fields.reserve(num+1); // +1 for safety
    for(std::size_t i=0; i < num; ++i) {
        auto ind = key_indices.at(i);
        fields.emplace_back(aggregate_input->at(ind));
        nullables.push_back(aggregate_input->nullable(ind));
    }
    std::size_t record_size = meta::record_meta::npos;
    if (! post) {
        fields.emplace_back(meta::field_type{enum_tag<kind::pointer>});
        nullables.push_back(true);
    } else {
        // post key doesn't have internal pointer field, but the record length is same as mid
        record_size = create_key_meta(output_kind::mid, key_indices, aggregate_input)->record_size();
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables),
        record_size
    );
}

std::shared_ptr<meta::record_meta> aggregate_info::output_info::create_value_meta(
    std::vector<aggregator_spec> const& aggregator_specs
) {
    auto num = aggregator_specs.size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type nullables(num);
    nullables.flip(); // assuming all values can be null
    fields.reserve(num);
    for(std::size_t i=0; i < num; ++i) {
        auto&& v = aggregator_specs[i];
        fields.emplace_back(v.type());
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables)
    );
}

std::vector<std::vector<field_locator>> aggregate_info::output_info::create_source_field_locators(
    std::vector<aggregator_spec> const& aggregator_specs,
    maybe_shared_ptr<meta::record_meta> const& phase_input
) {
    std::vector<std::vector<field_locator>> ret{};
    ret.reserve(aggregator_specs.size());
    for(auto&& as : aggregator_specs) {
        std::vector<field_locator> src_fields{};
        src_fields.reserve(as.argument_indices().size());
        for(auto i : as.argument_indices()) {
            src_fields.emplace_back(
                phase_input->at(i),
                phase_input->nullable(i),
                phase_input->value_offset(i),
                phase_input->nullity_offset(i)
            );
        }
        ret.emplace_back(std::move(src_fields));
    }
    return ret;
}

std::vector<field_locator> aggregate_info::output_info::create_target_field_locators(
    std::vector<aggregator_spec> const& aggregator_specs,
    maybe_shared_ptr<meta::group_meta> const& group_meta
) {
    std::vector<field_locator> ret{};
    ret.reserve(aggregator_specs.size());
    auto& value_meta = group_meta->value();
    for(std::size_t i=0, n=aggregator_specs.size(); i < n; ++i) {
        auto& as = aggregator_specs[i];
        ret.emplace_back(
            as.type(),
            value_meta.nullable(i),
            value_meta.value_offset(i),
            value_meta.nullity_offset(i)
        );
    }
    return ret;
}

field_locator const &aggregate_info::output_info::target_field_locator(std::size_t aggregator_index) const noexcept {
    return target_field_locators_[aggregator_index];
}

aggregate_info::output_info::output_info(
    aggregate_info::output_kind kind,
    std::vector<aggregator_spec> aggregator_specs,
    maybe_shared_ptr<meta::record_meta> const& aggregate_input,
    maybe_shared_ptr<meta::record_meta> phase_input,
    std::vector<field_index_type> const& key_indices
) :
    kind_(kind),
    aggregator_specs_(std::move(aggregator_specs)),
    phase_input_(std::move(phase_input)),
    group_(
        std::make_shared<meta::group_meta>(
            create_key_meta(kind_, key_indices, aggregate_input),
            create_value_meta(aggregator_specs_)
        )
    ),
    source_field_locators_(create_source_field_locators(aggregator_specs_, phase_input_)),
    target_field_locators_(create_target_field_locators(aggregator_specs_, group_)),
    key_compare_info_(group_->key())
{}

const maybe_shared_ptr<meta::group_meta> &aggregate_info::output_info::group_meta() const noexcept {
    return group_;
}

sequence_view<const aggregate_info::aggregator_spec> aggregate_info::output_info::aggregator_specs() const noexcept {
    return aggregator_specs_;
}

std::size_t aggregate_info::output_info::value_count() const noexcept {
    return aggregator_specs_.size();
}

sequence_view<const field_locator>
aggregate_info::output_info::source_field_locators(std::size_t aggregator_index) const noexcept {
    return source_field_locators_[aggregator_index];
}

const maybe_shared_ptr<meta::record_meta> &aggregate_info::extracted_key_meta() const noexcept {
    return extracted_key_meta_;
}

accessor::record_ref aggregate_info::output_key(accessor::record_ref mid) const noexcept {
    return accessor::record_ref(mid.data(), post_.group_meta()->key().record_size());
}

std::vector<meta::field_type> types(meta::record_meta const& meta, std::vector<std::size_t> const& indices) {
    std::vector<meta::field_type> ret{};
    ret.reserve(indices.size());
    for(auto i : indices) {
        ret.emplace_back(meta.at(i));
    }
    return ret;
}

aggregate_info::output_info aggregate_info::create_output(
    output_kind kind,
    std::vector<value_spec> const& value_specs,
    maybe_shared_ptr<meta::record_meta> const& phase_input,
    maybe_shared_ptr<meta::record_meta> const& aggregate_input,
    std::vector<field_index_type> const& key_indices
) {
    std::vector<aggregator_spec> aggregator_specs{};
    std::size_t agg_index = 0;
    for(auto const& vs : value_specs) {
        auto& info = vs.function_info();
        switch(kind) {
            case output_kind::pre: {
                auto aggs = info.pre();
                std::vector<size_t> arg_indices{vs.argument_indices().begin(), vs.argument_indices().end()};
                auto ts = types(*phase_input, arg_indices);
                auto seq = info.intermediate_types(ts);
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
                for(std::size_t i=0, n=aggs.size(); i < n; ++i, ++agg_index) {
                    std::vector<size_t> indices{agg_index};
                    aggregator_specs.emplace_back(
                        aggs[i],
                        indices,
                        phase_input->at(agg_index)
                    );
                }
                break;
            }
            case output_kind::post: {
                auto aggs = info.post();
                BOOST_ASSERT(aggs.size() == 1);  //NOLINT
                std::vector<size_t> indices{};
                for(std::size_t i=0, n=aggs[0].arg_count(); i < n; ++i, ++agg_index) {
                    indices.emplace_back(agg_index);
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
        kind,
        std::move(aggregator_specs),
        aggregate_input,
        phase_input,
        key_indices,
    };
}

const aggregate_info::output_info& aggregate_info::pre() const noexcept {
    return pre_;
}

const aggregate_info::output_info& aggregate_info::mid() const noexcept {
    return mid_;
}

const aggregate_info::output_info& aggregate_info::post() const noexcept {
    return post_;
}

bool aggregate_info::generate_record_on_empty() const noexcept {
    return generate_record_on_empty_;
}

aggregate_info::value_spec::value_spec(
    aggregate_function_info const& function_info,
    std::vector<std::size_t> argument_indices,
    meta::field_type type
) noexcept:
    function_info_(std::addressof(function_info)),
    argument_indices_(std::move(argument_indices)),
    type_(std::move(type))
{}

aggregate_function_info const &aggregate_info::value_spec::function_info() const noexcept {
    return *function_info_;
}

sequence_view<const std::size_t> aggregate_info::value_spec::argument_indices() const noexcept {
    return argument_indices_;
}

meta::field_type const &aggregate_info::value_spec::type() const noexcept {
    return type_;
}

aggregate_info::aggregator_spec::aggregator_spec(class aggregator_info const& aggregator_info,
    std::vector<std::size_t> argument_indices, meta::field_type type) noexcept:
    aggregator_info_(std::addressof(aggregator_info)),
    argument_indices_(std::move(argument_indices)),
    type_(std::move(type))
{}

class aggregator_info const &aggregate_info::aggregator_spec::aggregator_info() const noexcept {
    return *aggregator_info_;
}

sequence_view<const std::size_t> aggregate_info::aggregator_spec::argument_indices() const noexcept {
    return argument_indices_;
}

meta::field_type const &aggregate_info::aggregator_spec::type() const noexcept {
    return type_;
}

}
