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

namespace jogasaki::utils {

std::vector<yugawara::storage::index::key> keys(
    std::shared_ptr<yugawara::storage::table const> t,
    std::initializer_list<std::size_t> key_indices
) {
    std::vector<yugawara::storage::index::key> ret{};
    for (auto i : key_indices) {
        ret.emplace_back(t->columns()[i]);
    }
    return ret;
}
std::vector<yugawara::storage::index::column_ref> values(
    std::shared_ptr<yugawara::storage::table const> t,
    std::initializer_list<std::size_t> value_indices
) {
    std::vector<yugawara::storage::index::column_ref> ret{};
    for (auto i : value_indices) {
        ret.emplace_back(t->columns()[i]);
    }
    return ret;
}

// CREATE INDEX is not supported, so use this to create secondary directly
std::unique_ptr<kvs::storage> create_secondary_index(
    api::impl::database& db,
    std::string_view name,
    std::string_view base_table,
    std::initializer_list<std::size_t> key_indices,
    std::initializer_list<std::size_t> value_indices
) {
    auto provider = db.tables();
    auto t = provider->find_table(base_table);
    auto k = keys(t, std::move(key_indices));
    auto v = values(t, value_indices);
    {
        auto res = db.create_index(
            std::make_shared<yugawara::storage::index>(
                t,
                std::string{name},
                std::move(k),
                std::move(v),
                yugawara::storage::index_feature_set{
                    ::yugawara::storage::index_feature::find,
                    ::yugawara::storage::index_feature::scan,
                }
            )
        );
        [&]() { ASSERT_EQ(status::ok, res); }();
    }
    {
        auto s0 = provider->find_index(name);
        [&]() { ASSERT_TRUE(s0); }();
    }
    return db.kvs_db()->get_storage(name);
}

}

