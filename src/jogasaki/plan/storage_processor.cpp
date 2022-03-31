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

#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/storage/basic_prototype_processor.h>

namespace jogasaki::plan {

using ::yugawara::storage::index;
using ::yugawara::storage::table;
namespace schema = ::yugawara::schema;

bool
storage_processor::ensure(schema::declaration const& location, table& table_prototype, index& primary_index_prototype,
    yugawara::storage::prototype_processor::diagnostic_consumer_type const& diagnostic_consumer) {
    (void)location;
    (void)table_prototype;
    (void)primary_index_prototype;
    (void)diagnostic_consumer;

    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    std::vector<yugawara::storage::index::column_ref> cols{};
    for(auto&& cc : table_prototype.columns()) {
        bool contained = false;
        for(auto tc : primary_index_prototype.keys()) {
            if(tc == cc) {
                contained = true;
                break;
            }
        }
        if (contained) {
            continue;
        }
        cols.emplace_back(cc);
    }

    primary_index_prototype.simple_name(std::string{table_prototype.simple_name()});
    primary_index_prototype.values() = cols;
    primary_index_prototype.features() = index_features;
    return true;
}

bool storage_processor::ensure(schema::declaration const& location, index& secondary_index_prototype,
    yugawara::storage::prototype_processor::diagnostic_consumer_type const& diagnostic_consumer) {
    (void)location;
    (void)secondary_index_prototype;
    (void)diagnostic_consumer;
    //TODO implement and modify secondary indices
    return true;
}
}
