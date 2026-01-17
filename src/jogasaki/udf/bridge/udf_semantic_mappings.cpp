/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "udf_semantic_mappings.h"

#include <functional>
#include <unordered_map>

#include <takatori/type/blob.h>
#include <takatori/type/character.h>
#include <takatori/type/clob.h>
#include <takatori/type/date.h>
#include <takatori/type/datetime_interval.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/simple_type.h>
#include <takatori/type/table.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/function/udf_functions.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/udf/data/udf_semantic_type.h>
#include <jogasaki/udf/enum_types.h>
#include <jogasaki/udf/plugin_api.h>

namespace jogasaki::udf::bridge {

namespace { // anonymous
const std::unordered_map<plugin::udf::type_kind, jogasaki::udf::data::udf_semantic_type>&
udf_semantic_map() {
    using K   = plugin::udf::type_kind;
    using sem = jogasaki::udf::data::udf_semantic_type;

    static const std::unordered_map<K, sem> map{
        // boolean
        {K::boolean, sem::boolean},

        // int4 family
        {K::int4, sem::int4},
        {K::uint4, sem::int4},
        {K::sint4, sem::int4},
        {K::fixed4, sem::int4},
        {K::sfixed4, sem::int4},
        {K::grpc_enum, sem::int4},

        // int8 family
        {K::int8, sem::int8},
        {K::uint8, sem::int8},
        {K::sint8, sem::int8},
        {K::fixed8, sem::int8},
        {K::sfixed8, sem::int8},

        // float
        {K::float4, sem::float4},
        {K::float8, sem::float8},

        // text-like
        {K::string, sem::character},
        {K::group, sem::character},
        {K::message, sem::character},

        // binary
        {K::bytes, sem::octet},
    };
    return map;
}
const std::unordered_map<jogasaki::udf::data::udf_semantic_type, std::size_t>&
semantic_index_map() {
    using sem = jogasaki::udf::data::udf_semantic_type;
    using mf  = jogasaki::meta::field_type_kind;
    static const std::unordered_map<jogasaki::udf::data::udf_semantic_type, std::size_t> map{
        {sem::boolean, ::jogasaki::data::any::index<runtime_t<mf::boolean>>},
        {sem::int4, ::jogasaki::data::any::index<runtime_t<mf::int4>>},
        {sem::int8, ::jogasaki::data::any::index<runtime_t<mf::int8>>},
        {sem::float4, ::jogasaki::data::any::index<runtime_t<mf::float4>>},
        {sem::float8, ::jogasaki::data::any::index<runtime_t<mf::float8>>},
        {sem::character, ::jogasaki::data::any::index<jogasaki::accessor::text>},
        {sem::octet, ::jogasaki::data::any::index<jogasaki::accessor::binary>},
    };
    return map;
}

const std::unordered_map<jogasaki::udf::data::udf_semantic_type,
    std::function<std::shared_ptr<takatori::type::data const>()>>&
semantic_type_map() {

    namespace t = takatori::type;
    using sem   = jogasaki::udf::data::udf_semantic_type;

    static const std::unordered_map<sem, std::function<std::shared_ptr<t::data const>()>> map{
        {sem::boolean, [] { return std::make_shared<t::simple_type<t::type_kind::boolean>>(); }},
        {sem::int4, [] { return std::make_shared<t::simple_type<t::type_kind::int4>>(); }},
        {sem::int8, [] { return std::make_shared<t::simple_type<t::type_kind::int8>>(); }},
        {sem::float4, [] { return std::make_shared<t::simple_type<t::type_kind::float4>>(); }},
        {sem::float8, [] { return std::make_shared<t::simple_type<t::type_kind::float8>>(); }},
        {sem::character, [] { return std::make_shared<t::character>(t::varying); }},
        {sem::octet, [] { return std::make_shared<t::octet>(t::varying); }},
    };
    return map;
}

const std::unordered_map<jogasaki::udf::data::udf_semantic_type, jogasaki::meta::field_type_kind>&
semantic_meta_kind_map() {
    using sem = jogasaki::udf::data::udf_semantic_type;
    using k   = jogasaki::meta::field_type_kind;

    static const std::unordered_map<sem, k> map{
        {sem::boolean, k::boolean},
        {sem::int4, k::int4},
        {sem::int8, k::int8},
        {sem::float4, k::float4},
        {sem::float8, k::float8},
        {sem::character, k::character},
        {sem::octet, k::octet},
    };
    return map;
}
} // namespace
jogasaki::meta::field_type to_field_type(jogasaki::meta::field_type_kind k) {
    using mk = jogasaki::meta::field_type_kind;

    switch (k) {
        case mk::boolean: return jogasaki::meta::boolean_type();
        case mk::int4: return jogasaki::meta::int4_type();
        case mk::int8: return jogasaki::meta::int8_type();
        case mk::float4: return jogasaki::meta::float4_type();
        case mk::float8: return jogasaki::meta::float8_type();
        case mk::date: return jogasaki::meta::date_type();
        case mk::blob: return jogasaki::meta::blob_type();
        case mk::clob: return jogasaki::meta::clob_type();

        case mk::character: return jogasaki::meta::character_type();
        case mk::octet: return jogasaki::meta::octet_type();
        case mk::decimal: return jogasaki::meta::decimal_type();
        case mk::time_of_day: return jogasaki::meta::time_of_day_type();
        case mk::time_point: return jogasaki::meta::time_point_type();
        default: fail_with_exception_msg("unhandled meta::field_type_kind in make_field_type()");
    }
}
const std::unordered_map<plugin::udf::type_kind, std::size_t>& type_index_map() {
    using K = plugin::udf::type_kind;

    static const std::unordered_map<K, std::size_t> map = [] {
        std::unordered_map<K, std::size_t> m;

        auto const& sem_map = udf_semantic_map();
        auto const& idx_map = semantic_index_map();

        for (auto const& [k, sem] : sem_map) {
            if (auto it = idx_map.find(sem); it != idx_map.end()) { m.emplace(k, it->second); }
        }
        return m;
    }();

    return map;
}
std::shared_ptr<takatori::type::data const> to_takatori_type(plugin::udf::type_kind kind) {

    auto const& sem_map  = udf_semantic_map();
    auto const& type_map = semantic_type_map();

    auto sit = sem_map.find(kind);
    assert_with_exception(sit != sem_map.end(), kind);

    auto tit = type_map.find(sit->second);
    assert_with_exception(tit != type_map.end(), kind);

    return tit->second();
}

jogasaki::meta::field_type_kind to_meta_kind(plugin::udf::type_kind k) {

    auto const& sem_map = jogasaki::udf::bridge::udf_semantic_map();
    auto sit            = sem_map.find(k);
    assert_with_exception(sit != sem_map.end(), k);

    auto const& meta_map = jogasaki::udf::bridge::semantic_meta_kind_map();
    auto mit             = meta_map.find(sit->second);
    assert_with_exception(mit != meta_map.end(), k);

    return mit->second;
}

jogasaki::meta::field_type_kind to_meta_kind(plugin::udf::column_descriptor const& col) {
    return to_meta_kind(col.type_kind());
}

} // namespace jogasaki::udf::bridge
