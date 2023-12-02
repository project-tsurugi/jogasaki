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
#include "secondary_index.h"

#include <gtest/gtest.h>
#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/index/index_accessor.h>
#include <jogasaki/index/field_factory.h>

namespace jogasaki::utils {

using takatori::util::string_builder;
using takatori::util::throw_exception;

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

// use this to create secondary directly without using DDL
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

void copy_record(
    meta::record_meta const& src_meta,
    meta::record_meta const& dest_meta,
    accessor::record_ref src,
    accessor::record_ref dest,
    memory::paged_memory_resource* resource = nullptr
    ) {
    for(std::size_t i=0, n=src_meta.field_count(); i < n; ++i) {
        auto src_field = src_meta.at(i);
        auto dest_field = dest_meta.at(i);
        if(src_field != dest_field) {
            continue;
        }
        if(src_meta.nullable(i)) {
            utils::copy_nullable_field(src_field, dest, dest_meta.value_offset(i), dest_meta.nullity_offset(i),
                                       src, src_meta.value_offset(i), src_meta.nullity_offset(i), resource);
        } else {
            utils::copy_field(src_field, dest, dest_meta.value_offset(i), src, src_meta.value_offset(i), resource);
        }
    }
}

void validate_meta(
    meta::record_meta const& src_meta,
    meta::record_meta const& dest_meta
) {
    if(src_meta.field_count() != dest_meta.field_count()) {
        throw_exception(std::runtime_error{
            string_builder{} << "field count differs " << src_meta.field_count() << " != " << dest_meta.field_count()
                             << string_builder::to_string
        });
    }
    for(std::size_t i=0, n=src_meta.field_count(); i < n; ++i) {
        auto src_field = src_meta.at(i);
        auto dest_field = dest_meta.at(i);
        if(src_field != dest_field) {
            throw_exception(std::runtime_error{
                string_builder{} << "type mismatch at field " << i << ": " << src_field << " != " << dest_field
                               << string_builder::to_string
            });
        }
        if(src_meta.nullable(i) != dest_meta.nullable(i)) {
            throw_exception(std::runtime_error{
                string_builder{} << "nullity mismatch at field " << i << ": " << src_meta.nullable(i) << " != " << dest_meta.nullable(i)
                               << string_builder::to_string
            });
        }
    }
}
std::vector<std::pair<mock::basic_record, mock::basic_record>> get_secondary_entries(
    kvs::database& db,
    yugawara::storage::index const& primary,
    yugawara::storage::index const& secondary,
    mock::basic_record const& secondary_key_template,
    mock::basic_record const& primary_key_template
) {
    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    {
        mock::basic_record secondary_key{secondary_key_template};
        mock::basic_record primary_key{primary_key_template};
        std::vector<std::pair<mock::basic_record, mock::basic_record>> ret{};
        {
            data::aligned_buffer buf{};
            auto tx = wrap(db.create_transaction());

            std::unique_ptr<kvs::iterator> it{};
            auto stg = db.get_storage(secondary.simple_name());
            if(status::ok != stg->scan(*tx, buf, kvs::end_point_kind::unbound, buf, kvs::end_point_kind::unbound, it)) {
                fail();
            };

            auto secondary_key_meta = jogasaki::index::create_meta(secondary, true);
            auto primary_key_meta = jogasaki::index::create_meta(primary, true);
            validate_meta(*secondary_key.record_meta(), *secondary_key_meta);
            validate_meta(*primary_key.record_meta(), *primary_key_meta);

            auto secondary_key_fields = jogasaki::index::index_fields(secondary, true);
            auto primary_key_fields = jogasaki::index::index_fields(primary, true);
            jogasaki::index::mapper secondary_mapper{secondary_key_fields, {}};
            jogasaki::index::mapper primary_mapper{primary_key_fields, {}};

            data::small_record_store secondary_key_store{secondary_key_meta, &resource};
            data::small_record_store primary_key_store{primary_key_meta, &resource};
            while (status::ok == it->next()) {
                std::string_view key{};
                if(status::ok != it->key(key)) {
                    fail();
                };
                DVLOG(log_trace) << "key: " << binary_printer{key};
                kvs::readable_stream in{key.data(), key.size()};
                secondary_mapper.read(true, in, secondary_key_store.ref(), &resource);
                copy_record(*secondary_key_meta, *secondary_key.record_meta(), secondary_key_store.ref(), secondary_key.ref(), &resource);
                primary_mapper.read(true, in, primary_key_store.ref(), &resource);
                copy_record(*primary_key_meta, *primary_key.record_meta(), primary_key_store.ref(), primary_key.ref(), &resource);
                ret.emplace_back(std::move(secondary_key), std::move(primary_key));
            }
            it.reset();
            if(status::ok != tx->commit()) {
                fail();
            }
        }
        return ret;
    }
}
}

