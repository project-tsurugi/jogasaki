/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <memory>
#include <initializer_list>
#include <string_view>

#include <yugawara/storage/table.h>
#include <yugawara/storage/index.h>

#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api/impl/database.h>

namespace jogasaki::utils {

std::vector<yugawara::storage::index::key> keys(
    std::shared_ptr<yugawara::storage::table const> t,
    std::initializer_list<std::size_t> key_indices
);

std::vector<yugawara::storage::index::column_ref> values(
    std::shared_ptr<yugawara::storage::table const> t,
    std::initializer_list<std::size_t> value_indices
);

// use this to create secondary directly without using DDL
std::unique_ptr<kvs::storage> create_secondary_index(
    api::impl::database& db,
    std::string_view name,
    std::string_view base_table,
    std::initializer_list<std::size_t> key_indices,
    std::initializer_list<std::size_t> value_indices
);

std::vector<std::pair<mock::basic_record, mock::basic_record>> get_secondary_entries(
    kvs::database& db,
    yugawara::storage::index const& primary,
    yugawara::storage::index const& secondary,
    mock::basic_record const& secondary_key_template,
    mock::basic_record const& primary_key_template);
}

