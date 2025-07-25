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
#include "storage_processor.h"

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <takatori/tree/tree_element_vector.h>
#include <takatori/type/data.h>
#include <takatori/type/decimal.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/column_feature.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/constants.h>
#include <jogasaki/utils/map_schema_name.h>

namespace jogasaki::plan {

using ::yugawara::storage::index;
using ::yugawara::storage::table;
namespace schema = ::yugawara::schema;
using ::takatori::util::unsafe_downcast;

bool contains(std::vector<index::key>& keys, yugawara::storage::column& c) {
    bool contained = false;
    for(auto&& tc : keys) {
        if(tc == c) {
            contained = true;
            break;
        }
    }
    return contained;
}

void fill_generated_sequence(
    yugawara::storage::column& cc,
    storage_processor::generated_sequences_type& generated_sequences,
    std::string_view location_name,
    std::string_view table_name
) {
    if(cc.default_value().kind() == ::yugawara::storage::column_value_kind::sequence) {
        auto& ro = cc.default_value().element<yugawara::storage::column_value_kind::sequence>();
        auto name = std::string(generated_sequence_name_prefix) + "_" +
            std::string{utils::map_schema_name_to_storage_namespace(location_name)} + "_" +
            std::string{table_name} + "_" +
            std::string{cc.simple_name()};

        auto seq = std::make_shared<yugawara::storage::sequence>(
            name,
            ro->initial_value(),
            ro->increment_value(),
            ro->min_value(),
            ro->max_value(),
            ro->cycle()
        );
        generated_sequences.emplace_back(seq);
        cc.default_value() = {std::move(seq)};
    }
}

void storage_processor::add_pk_column_if_not_exists(
    table& table_prototype,
    index& primary_index_prototype,
    std::string_view location_name,
    std::string_view table_name
) {
    if(primary_index_prototype.keys().empty()) {
        primary_key_generated_ = true;
        auto name = std::string(generated_pkey_column_prefix) + "_" +
            std::string{utils::map_schema_name_to_storage_namespace(location_name)} + "_" +
            std::string{table_name};
        auto seq = std::make_shared<yugawara::storage::sequence>(name);
        auto& c = table_prototype.columns().emplace_back(
            yugawara::storage::column{
                name,
                takatori::type::int8(),
                yugawara::variable::nullity{false},
                {seq},
                yugawara::storage::column::feature_set_type{
                    yugawara::storage::column_feature::synthesized,
                    yugawara::storage::column_feature::hidden
                }
            }
        );
        primary_index_prototype.keys().emplace_back(c);
        primary_key_sequence_ = seq;
    }
}

bool storage_processor::ensure(
    schema::declaration const& location,
    table& table_prototype,
    index& primary_index_prototype,
    yugawara::storage::prototype_processor::diagnostic_consumer_type const& diagnostic_consumer
) {
    (void)diagnostic_consumer;

    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };

    std::vector<yugawara::storage::index::column_ref> cols{};
    generated_sequences_.reserve(table_prototype.columns().size());
    for(auto&& cc : table_prototype.columns()) {
        // fill decimal precision default
        if(cc.type().kind() == ::takatori::type::type_kind::decimal) {
            auto& t = unsafe_downcast<::takatori::type::decimal>(cc.type());
            if(! t.precision().has_value()) {
                cc.type(std::make_shared<takatori::type::decimal>(decimal_default_precision, t.scale()));
            }
        }

        // put non-key columns as primary index values
        if(! contains(primary_index_prototype.keys(), cc)) {
            cols.emplace_back(cc);
        }

        fill_generated_sequence(cc, generated_sequences_, location.name(), table_prototype.simple_name());
    }

    // pk generation here in order not to conflict with generated sequences above
    add_pk_column_if_not_exists(
        table_prototype,
        primary_index_prototype,
        location.name(),
        table_prototype.simple_name()
    );

    primary_index_prototype.simple_name(std::string{table_prototype.simple_name()});
    primary_index_prototype.values() = cols;
    primary_index_prototype.features() = index_features;

    return true;
}

bool storage_processor::ensure(
    schema::declaration const& location,
    index& secondary_index_prototype,
    yugawara::storage::prototype_processor::diagnostic_consumer_type const& diagnostic_consumer
) {
    (void)location;
    (void)diagnostic_consumer;
    yugawara::storage::index_feature_set secondary_index_features{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
    };
    secondary_index_prototype.features() = secondary_index_features;
    return true;
}

storage_processor_result storage_processor::result() const noexcept {
    return storage_processor_result(primary_key_generated_, primary_key_sequence_, generated_sequences_);
}

storage_processor_result::storage_processor_result(
    bool primary_key_generated,
    std::shared_ptr<yugawara::storage::sequence> primary_key_sequence,
    generated_sequences_type generated_sequences
) :
    primary_key_generated_(primary_key_generated),
    primary_key_sequence_(std::move(primary_key_sequence)),
    generated_sequences_(std::move(generated_sequences))
{}

bool storage_processor_result::primary_key_generated() const noexcept {
    return primary_key_generated_;
}

std::shared_ptr<yugawara::storage::sequence> storage_processor_result::primary_key_sequence() const noexcept {
    return primary_key_sequence_;
}

[[nodiscard]] storage_processor_result::generated_sequences_type storage_processor_result::generated_sequences() const noexcept {
    return generated_sequences_;
}

}  // namespace jogasaki::plan
