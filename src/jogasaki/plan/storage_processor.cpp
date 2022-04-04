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
#include "storage_processor.h"

#include <atomic>

#include <takatori/type/int.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/storage/basic_prototype_processor.h>
#include <yugawara/storage/sequence.h>

#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/sequence.h>

namespace jogasaki::plan {

using ::yugawara::storage::index;
using ::yugawara::storage::table;
namespace schema = ::yugawara::schema;

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

constexpr static sequence_definition_id generated_sequence_definition_id_begin = 1000000;
std::atomic_size_t generated_pkey_definition_id_src{generated_sequence_definition_id_begin};

bool storage_processor::ensure(
    schema::declaration const& location,
    table& table_prototype,
    index& primary_index_prototype,
    yugawara::storage::prototype_processor::diagnostic_consumer_type const& diagnostic_consumer
) {
    (void)location;
    (void)diagnostic_consumer;

    if(primary_index_prototype.keys().empty()) {
        primary_key_generated_ = true;
        auto defid = generated_pkey_definition_id_src++;
        auto name = std::string(generated_pkey_column_prefix)+std::to_string(defid);
        auto seq = std::make_shared<yugawara::storage::sequence>(defid, name);
        auto& c = table_prototype.columns().emplace_back(
            yugawara::storage::column{name, takatori::type::int8(), yugawara::variable::nullity{false}, {seq}}
        );
        primary_index_prototype.keys().emplace_back(c);
        primary_key_sequence_ = seq;

        // For now, generated def id is valid only the DBMS is up and running. After recovery, different one might be used.
        //TODO save metadata (mapping def id and column that uses the sequence) for the consistency after recovery
    }

    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };

    std::vector<yugawara::storage::index::column_ref> cols{};
    for(auto&& cc : table_prototype.columns()) {
        if(! contains(primary_index_prototype.keys(), cc)) {
            cols.emplace_back(cc);
        }
    }

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
    (void)secondary_index_prototype;
    (void)diagnostic_consumer;
    //TODO implement and modify secondary indices
    return true;
}

storage_processor_result storage_processor::result() const noexcept {
    return storage_processor_result(primary_key_generated_, primary_key_sequence_);
}

storage_processor_result::storage_processor_result(bool primary_key_generated,
    std::shared_ptr<yugawara::storage::sequence const> primary_key_sequence) :
    primary_key_generated_(primary_key_generated),
    primary_key_sequence_(std::move(primary_key_sequence))
{}

bool storage_processor_result::primary_key_generated() const noexcept {
    return primary_key_generated_;
}

std::shared_ptr<yugawara::storage::sequence const> storage_processor_result::primary_key_sequence() const noexcept {
    return primary_key_sequence_;
}
}
