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
#include "storage_data.h"

#include <fstream>
#include <glog/logging.h>
#include <boost/filesystem.hpp>

#include <takatori/util/fail.h>
#include <takatori/type/character.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/logging.h>
#include <jogasaki/data/value.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/utils/random.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/storage_dump.h>
#include <jogasaki/error.h>
#include <jogasaki/api.h>
#include <jogasaki/utils/create_tx.h>

namespace jogasaki::utils {

using namespace std::string_literals;
using namespace std::string_view_literals;
using kind = meta::field_type_kind;
using takatori::util::fail;
using yugawara::storage::configurable_provider;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::expression;

using value = data::value;

namespace {

template <class T>
value create_value(
    std::size_t val,
    std::size_t record_count,
    bool nullable,
    std::size_t len = -1
) {
    (void) len;
    if (nullable && record_count % 5 == 0) {
        return {};
    }
    return data::value{std::in_place_type<T>, val};
}

template <>
value create_value<runtime_t<meta::field_type_kind::character>>(
    std::size_t val,
    std::size_t record_count,
    bool nullable,
    std::size_t len
) {
    char c = 'A' + val % 26;
    std::string d(len, c);
    if (nullable && record_count % 5 == 0) {
        return data::value{std::in_place_type<std::string>, {}};
    }
    return data::value{std::in_place_type<std::string>, d};
}

template <>
value create_value<runtime_t<meta::field_type_kind::date>>(
    std::size_t val,
    std::size_t record_count,
    bool nullable,
    std::size_t len
) {
    (void) len;
    if (nullable && record_count % 5 == 0) {
        return {};
    }
    return data::value{std::in_place_type<runtime_t<meta::field_type_kind::date>>, val};
}

template <>
value create_value<runtime_t<meta::field_type_kind::time_of_day>>(
    std::size_t val,
    std::size_t record_count,
    bool nullable,
    std::size_t len
) {
    (void) len;
    if (nullable && record_count % 5 == 0) {
        return {};
    }
    return data::value{std::in_place_type<runtime_t<meta::field_type_kind::time_of_day>>, std::chrono::nanoseconds{val}};
}

template <>
value create_value<runtime_t<meta::field_type_kind::time_point>>(
    std::size_t val,
    std::size_t record_count,
    bool nullable,
    std::size_t len
) {
    (void) len;
    if (nullable && record_count % 5 == 0) {
        return {};
    }
    return data::value{std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, std::chrono::seconds{val}};
}

template <class T>
std::string to_string(T arg) {
    std::stringstream ss{};
    ss << arg;
    return ss.str();
}

inline void encode_field(
    data::value const& a,
    meta::field_type f,
    kvs::coding_spec spec,
    bool nullable,
    kvs::writable_stream& target
) {
    if (nullable) {
        kvs::encode_nullable(a.view(), f, spec, target);
        return;
    }
    kvs::encode(a.view(), f, spec, target);
}

static void fill_fields(
    meta::record_meta const& meta,
    kvs::writable_stream& target,
    bool key,
    std::size_t record_count,
    bool sequential,
    std::size_t modulo,
    utils::xorshift_random64& rnd,
    std::vector<bool> const& key_order_asc = {}
) {
    std::size_t field_index = 0;
    for(auto&& f: meta) {
        auto spec = key ?
            (key_order_asc[field_index] ? kvs::spec_key_ascending : kvs::spec_key_descending) :
            kvs::spec_value;
        bool nullable = meta.nullable(field_index);
        std::size_t val = (sequential ? record_count : rnd()) % modulo;
        std::size_t len = 1 + (sequential ? record_count : rnd()) % 70;
        switch(f.kind()) {
            case kind::int4: {
                data::value a{create_value<std::int32_t>(val, record_count, nullable)};
                encode_field(a, meta::field_type(meta::field_enum_tag<kind::int4>), spec, nullable, target);
                break;
            }
            case kind::int8: {
                data::value a{create_value<std::int64_t>(val, record_count, nullable)};
                encode_field(a, meta::field_type(meta::field_enum_tag<kind::int8>), spec, nullable, target);
                break;
            }
            case kind::float4: {
                data::value a{create_value<float>(val, record_count, nullable)};
                encode_field(a, meta::field_type(meta::field_enum_tag<kind::float4>), spec, nullable, target);
                break;
            }
            case kind::float8: {
                data::value a{create_value<double>(val, record_count, nullable)};
                encode_field(a, meta::field_type(meta::field_enum_tag<kind::float8>), spec, nullable, target);
                break;
            }
            case kind::character: {
                data::value a{create_value<runtime_t<meta::field_type_kind::character>>(val, record_count, nullable, len)};
                encode_field(a, meta::field_type(meta::field_enum_tag<kind::character>), spec, nullable, target);
                break;
            }
            case kind::decimal: {
                data::value a{create_value<runtime_t<meta::field_type_kind::decimal>>(val, record_count, nullable)};
                encode_field(a, meta::field_type(std::make_shared<meta::decimal_field_option>()), spec, nullable, target);
                break;
            }
            case kind::date: {
                data::value a{create_value<runtime_t<meta::field_type_kind::date>>(val, record_count, nullable)};
                encode_field(a, meta::field_type(meta::field_enum_tag<kind::date>), spec, nullable, target);
                break;
            }
            case kind::time_of_day: {
                data::value a{create_value<runtime_t<meta::field_type_kind::time_of_day>>(val, record_count, nullable)};
                encode_field(a, meta::field_type(std::make_shared<meta::time_point_field_option>()), spec, nullable, target);
                break;
            }
            case kind::time_point: {
                data::value a{create_value<runtime_t<meta::field_type_kind::time_point>>(val, record_count, nullable)};
                encode_field(a, meta::field_type(std::make_shared<meta::time_point_field_option>()), spec, nullable, target);
                break;
            }
            default:
                fail();
                break;
        }
        ++field_index;
    }
}

std::string any_to_string(value const& value, meta::field_type type) {
    if (! value) {
        return "NULL";
    }
    switch(type.kind()) {
        case kind::int4: return std::to_string(value.to<std::int32_t>());
        case kind::int8: return std::to_string(value.to<std::int64_t>());
        case kind::float4: return std::to_string(value.to<float>());
        case kind::float8: return std::to_string(value.to<double>());
        case kind::character: return std::string(1, '\'') + static_cast<std::string>(value.to<accessor::text>()) + std::string(1, '\'');
        case kind::decimal: return to_string(value.to<runtime_t<meta::field_type_kind::decimal>>());
        case kind::date: return to_string(value.to<runtime_t<meta::field_type_kind::date>>());
        case kind::time_of_day: return to_string(value.to<runtime_t<meta::field_type_kind::time_of_day>>());
        case kind::time_point: return to_string(value.to<runtime_t<meta::field_type_kind::time_point>>());
        default: break;
    }
    fail();
}

}  // namespace

void populate_storage_data(kvs::database* db, std::shared_ptr<configurable_provider> const& provider,
    std::string_view storage_name, std::size_t records_per_partition, bool sequential_data, std::size_t modulo) {
    auto stg = db->get_or_create_storage(storage_name);
    static std::size_t buflen = 1024*8;
    std::string key_buf(buflen, '\0');
    std::string val_buf(buflen, '\0');
    kvs::writable_stream key_stream{key_buf};
    kvs::writable_stream val_stream{val_buf};

    auto idx = provider->find_index(storage_name);

    std::vector<meta::field_type> flds{};
    boost::dynamic_bitset<std::uint64_t> nullabilities{};
    std::vector<bool> key_order_asc{};
    for(auto&& k : idx->keys()) {
        flds.emplace_back(utils::type_for(k.column().type()));
        nullabilities.push_back(k.column().criteria().nullity().nullable());
        key_order_asc.emplace_back(k.direction() == yugawara::storage::sort_direction::ascendant);
    }
    meta::record_meta key_meta{std::move(flds), std::move(nullabilities)};
    flds.clear();
    nullabilities.clear();
    for(auto&& v : idx->values()) {
        auto& c = static_cast<yugawara::storage::column const&>(v);
        flds.emplace_back(utils::type_for(c.type()));
        nullabilities.push_back(c.criteria().nullity().nullable());
    }
    meta::record_meta val_meta{std::move(flds), std::move(nullabilities)};

    static std::size_t record_per_transaction = 10000;
    std::unique_ptr<kvs::transaction> tx{};
    utils::xorshift_random64 rnd{};
    for(std::size_t i=0, n=records_per_partition; i < n; ++i) {
        if (! tx) {
            tx = db->create_transaction();
        }
        fill_fields(key_meta, key_stream, true, i, sequential_data, -1, rnd, key_order_asc);
        fill_fields(val_meta, val_stream, false, i, sequential_data, modulo, rnd);
        if(auto res = stg->put(*tx,
                std::string_view{key_buf.data(), key_stream.size()},
                std::string_view{val_buf.data(), val_stream.size()}
            ); ! is_ok(res)) {
            fail();
        }
        key_stream.reset();
        val_stream.reset();
        if (i == n-1 || (i != 0 && (i % record_per_transaction) == 0)) {
            if (auto res = tx->commit(); res != status::ok) {
                fail();
            }
            VLOG(log_trace) << "committed after " << i << "-th record";
            tx.reset();
        }
    }
}

void load_storage_data(api::database& db, std::shared_ptr<configurable_provider> const& provider,
    std::string_view table_name, std::size_t records_per_partition, bool sequential_data, std::size_t modulo) {
    auto table = provider->find_table(table_name);
    if (! table) {
        fail();
    }
    static std::size_t record_per_transaction = 10000;
    std::shared_ptr<api::transaction_handle> tx{};
    std::size_t record_count = 0;
    utils::xorshift_random64 rnd{};
    for(std::size_t i=0, n=records_per_partition; i < n; ++i) {
        if (! tx) {
            tx = utils::create_transaction(db);
        }
        std::vector<std::string> colnames{};
        std::vector<std::string> values{};
        for(auto&& k : table->columns()) {
            std::size_t val = (sequential_data ? record_count : rnd()) % modulo;
            colnames.emplace_back(k.simple_name());
            auto nullable = k.criteria().nullity().nullable();
            auto type = utils::type_for(k.type());
            switch(type.kind()) {
                case kind::int4: {
                    data::value a{create_value<std::int32_t>(val, record_count, nullable)};
                    values.emplace_back(any_to_string(a, type));
                    break;
                }
                case kind::int8: {
                    data::value a{create_value<std::int64_t>(val, record_count, nullable)};
                    values.emplace_back(any_to_string(a, type));
                    break;
                }
                case kind::float4: {
                    data::value a{create_value<float>(val, record_count, nullable)};
                    values.emplace_back(any_to_string(a, type));
                    break;
                }
                case kind::float8: {
                    data::value a{create_value<double>(val, record_count, nullable)};
                    values.emplace_back(any_to_string(a, type));
                    break;
                }
                case kind::character: {
                    auto& ct = takatori::util::unsafe_downcast<takatori::type::character>(k.type());
                    auto len = ct.length() ? *ct.length() : 1;
                    data::value a{create_value<runtime_t<meta::field_type_kind::character>>(val, record_count, nullable, len)};
                    values.emplace_back(any_to_string(a, type));
                    break;
                }
                case kind::date: {
                    data::value a{create_value<runtime_t<meta::field_type_kind::date>>(val, record_count, nullable)};
                    values.emplace_back(any_to_string(a, type));
                    break;
                }
                case kind::time_of_day: {
                    data::value a{create_value<runtime_t<meta::field_type_kind::time_of_day>>(val, record_count, nullable)};
                    values.emplace_back(any_to_string(a, type));
                    break;
                }
                case kind::time_point: {
                    data::value a{create_value<runtime_t<meta::field_type_kind::time_point>>(val, record_count, nullable)};
                    values.emplace_back(any_to_string(a, type));
                    break;
                }
                default:
                    fail();
                    break;
            }
        }

        std::stringstream ss{};
        ss << "INSERT INTO ";
        ss << table_name;
        ss << " (";
        bool first = true;
        for(auto&& cname : colnames) {
            if (! first) {
                ss << ", ";
            }
            first = false;
            ss << cname;
        }
        ss << ") VALUES (";
        first = true;
        for(auto&& v: values) {
            if (! first) {
                ss << ", ";
            }
            first = false;
            ss << v;
        }
        ss << ")";
//        // uncomment when debugging
//        LOG(INFO) << ss.str();

        std::unique_ptr<api::executable_statement> stmt{};
        if(auto res = db.create_executable(ss.str(), stmt); res != status::ok) {
            fail();
        }
        if(auto res = tx->execute(*stmt); res != status::ok && res != status::err_unique_constraint_violation) {
            fail();
        }
        if (i == n-1 || (i != 0 && (i % record_per_transaction) == 0)) {
            if (auto res = tx->commit(); res != status::ok) {
                fail();
            }
            VLOG(log_trace) << "committed after " << i << "-th record";
            tx.reset();
        }
        ++record_count;
    }
}
} //namespace
