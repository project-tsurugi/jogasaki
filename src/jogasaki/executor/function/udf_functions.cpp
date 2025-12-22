/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "udf_functions.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <boost/algorithm/string.hpp>
#include <boost/assert.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <tsl/hopscotch_hash.h>
#include <tsl/hopscotch_set.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/type/blob.h>
#include <takatori/type/character.h>
#include <takatori/type/clob.h>
#include <takatori/type/date.h>
#include <takatori/type/datetime_interval.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>
#include <yugawara/util/maybe_shared_lock.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/builtin_scalar_functions_id.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/scalar_function_info.h>
#include <jogasaki/executor/function/scalar_function_kind.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/function/value_generator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/udf/enum_types.h>
#include <jogasaki/udf/error_info.h>
#include <jogasaki/udf/generic_record_impl.h>
#include <jogasaki/udf/plugin_loader.h>
#include <jogasaki/udf/udf_loader.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/assign_reference_tag.h>
#include <jogasaki/utils/base64_utils.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/round.h>
#include <jogasaki/utils/string_utils.h>

namespace jogasaki::executor::function {

using executor::expr::evaluator_context;
using takatori::util::sequence_view;
using kind = meta::field_type_kind;
using jogasaki::executor::expr::error;
using jogasaki::executor::expr::error_kind;
namespace {
constexpr std::size_t SUPPORTED_MAJOR = 0;
constexpr std::size_t SUPPORTED_MINOR = 1;
constexpr std::string_view DECIMAL_RECORD = "tsurugidb.udf.Decimal";
constexpr std::string_view DATE_RECORD = "tsurugidb.udf.Date";
constexpr std::string_view LOCALTIME_RECORD = "tsurugidb.udf.LocalTime";
constexpr std::string_view LOCALDATETIME_RECORD = "tsurugidb.udf.LocalDatetime";
constexpr std::string_view OFFSETDATETIME_RECORD = "tsurugidb.udf.OffsetDatetime";
constexpr std::string_view BLOB_RECORD = "tsurugidb.udf.BlobReference";
constexpr std::string_view CLOB_RECORD = "tsurugidb.udf.ClobReference";

const std::unordered_map<plugin::udf::type_kind_type, std::size_t>& type_index_map() {
    using K = plugin::udf::type_kind_type;
    static const std::unordered_map<K, std::size_t> map = {
        {K::float8, data::any::index<runtime_t<kind::float8>>},
        {K::float4, data::any::index<runtime_t<kind::float4>>},
        {K::int8, data::any::index<runtime_t<kind::int8>>},
        {K::sint8, data::any::index<runtime_t<kind::int8>>},
        {K::sfixed8, data::any::index<runtime_t<kind::int8>>},
        {K::uint8, data::any::index<runtime_t<kind::int8>>},
        {K::fixed8, data::any::index<runtime_t<kind::int8>>},
        {K::int4, data::any::index<runtime_t<kind::int4>>},
        {K::sint4, data::any::index<runtime_t<kind::int4>>},
        {K::sfixed4, data::any::index<runtime_t<kind::int4>>},
        {K::uint4, data::any::index<runtime_t<kind::int4>>},
        {K::fixed4, data::any::index<runtime_t<kind::int4>>},
        {K::boolean, data::any::index<runtime_t<kind::boolean>>},
        {K::group, data::any::index<accessor::text>},
        {K::bytes, data::any::index<accessor::binary>},
    };
    return map;
}

std::unordered_map<std::string_view, std::size_t> const& nested_type_map() {
    static const std::unordered_map<std::string_view, std::size_t> map{
        {DECIMAL_RECORD, data::any::index<runtime_t<kind::decimal>>},
        {DATE_RECORD, data::any::index<runtime_t<kind::date>>},
        {LOCALTIME_RECORD, data::any::index<runtime_t<kind::time_of_day>>},
        {LOCALDATETIME_RECORD, data::any::index<runtime_t<kind::time_point>>},
        {OFFSETDATETIME_RECORD, data::any::index<runtime_t<kind::time_point>>},
        {BLOB_RECORD, data::any::index<runtime_t<kind::blob>>},
        {CLOB_RECORD, data::any::index<runtime_t<kind::clob>>},
    };
    return map;
}
std::unordered_map<std::string_view, std::function<std::shared_ptr<const takatori::type::data>()>> const&
get_type_map() {
    static const std::unordered_map<std::string_view, std::function<std::shared_ptr<const takatori::type::data>()>>
        map = {
            {DECIMAL_RECORD, [] { return std::make_shared<takatori::type::decimal>(); }},
            {DATE_RECORD, [] { return std::make_shared<takatori::type::date>(); }},
            {LOCALTIME_RECORD, [] { return std::make_shared<takatori::type::time_of_day>(); }},
            {LOCALDATETIME_RECORD, [] { return std::make_shared<takatori::type::time_point>(); }},
            {OFFSETDATETIME_RECORD,
             [] { return std::make_shared<takatori::type::time_point>(takatori::type::with_time_zone); }},
            {BLOB_RECORD, [] { return std::make_shared<takatori::type::blob>(); }},
            {CLOB_RECORD, [] { return std::make_shared<takatori::type::clob>(); }},
        };
    return map;
}

[[nodiscard]]
bool is_supported_version(plugin::udf::package_version const& v) noexcept {
    return v.major() == SUPPORTED_MAJOR &&
           v.minor() == SUPPORTED_MINOR;
}

std::string int128_to_bytes(__int128 coeff) {
    std::string bytes(16, '\0');
    auto u = static_cast<unsigned __int128>(coeff);
    // NOLINTNEXTLINE(hicpp-signed-bitwise)
    for(int i = 0; i < 16; ++i) { bytes[15 - i] = static_cast<char>((u >> (i * 8)) & 0xFFU); }
    return bytes;
}

std::shared_ptr<takatori::type::data const> map_type(plugin::udf::type_kind_type kind) {
    namespace t = takatori::type;
    using K = plugin::udf::type_kind_type;
    switch(kind) {
        case K::float8: return std::make_shared<t::simple_type<t::type_kind::float8>>();
        case K::float4: return std::make_shared<t::simple_type<t::type_kind::float4>>();
        case K::int8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        case K::uint8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        case K::int4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::fixed8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        case K::fixed4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::boolean: return std::make_shared<t::simple_type<t::type_kind::boolean>>();
        case K::string: return std::make_shared<t::character>(t::varying);
        case K::group: return std::make_shared<t::character>(t::varying);
        case K::message: return std::make_shared<t::character>(t::varying);
        case K::bytes: return std::make_shared<t::octet>(t::varying);
        case K::uint4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::grpc_enum: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::sfixed4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::sfixed8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        case K::sint4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::sint8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        default: return std::make_shared<t::character>(t::varying);
    }
}

void register_function(
    yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo,
    yugawara::function::declaration::definition_id_type& current_id,
    std::string const& fn_name,
    std::shared_ptr<const takatori::type::data> const& return_type,
    std::vector<std::shared_ptr<const takatori::type::data>> const& param_types,
    std::function<data::any(evaluator_context&, sequence_view<data::any>)> const& lambda_func
) {
    current_id++;
    auto info =
        std::make_shared<scalar_function_info>(scalar_function_kind::user_defined, lambda_func, param_types.size());
    repo.add(current_id, info);
    functions.add(yugawara::function::declaration(current_id, fn_name, return_type, param_types));
}

void fill_request_with_args(  //NOLINT(readability-function-cognitive-complexity)
    plugin::udf::generic_record_impl& request,
    sequence_view<data::any> args,
    std::vector<plugin::udf::column_descriptor*> const& columns
) {
    for(std::size_t i = 0; i < columns.size(); ++i) {
        auto const& type = columns[i]->type_kind();
        auto const& src = args[i];
        switch(src.type_index()) {
            case data::any::index<runtime_t<kind::boolean>>: {
                request.add_bool(static_cast<bool>(src.to<runtime_t<kind::boolean>>()));
                break;
            }
            case data::any::index<runtime_t<kind::int4>>: {
                auto result = src.to<runtime_t<kind::int4>>();
                if(type == plugin::udf::type_kind_type::int4 || type == plugin::udf::type_kind_type::sfixed4 ||
                   type == plugin::udf::type_kind_type::sint4) {
                    request.add_int4(result);
                } else {
                    request.add_uint4(result);
                }
                break;
            }
            case data::any::index<runtime_t<kind::int8>>: {
                auto result = src.to<runtime_t<kind::int8>>();
                if(type == plugin::udf::type_kind_type::int8 || type == plugin::udf::type_kind_type::sfixed8 ||
                   type == plugin::udf::type_kind_type::sint8) {
                    request.add_int8(result);
                } else {
                    request.add_uint8(result);
                }
                break;
            }
            case data::any::index<runtime_t<kind::float4>>: request.add_float(src.to<runtime_t<kind::float4>>()); break;
            case data::any::index<runtime_t<kind::float8>>:
                request.add_double(src.to<runtime_t<kind::float8>>());
                break;
            case data::any::index<accessor::binary>: {
                auto bin = static_cast<std::string>(src.to<runtime_t<kind::octet>>());
                request.add_string(bin);
                break;
            }
            case data::any::index<accessor::text>: {
                auto ch = static_cast<std::string>(src.to<runtime_t<kind::character>>());
                request.add_string(ch);
                break;
            }
            case data::any::index<runtime_t<kind::decimal>>: {
                auto value = src.to<runtime_t<kind::decimal>>();
                std::int8_t sign = value.sign();
                std::uint64_t hi = value.coefficient_high();
                std::uint64_t lo = value.coefficient_low();
                std::int32_t exp = value.exponent();
                // NOLINTNEXTLINE(hicpp-signed-bitwise)
                unsigned __int128 ucoeff = (static_cast<unsigned __int128>(hi) << 64) | lo;
                __int128 coeff = (sign < 0) ? -static_cast<__int128>(ucoeff) : static_cast<__int128>(ucoeff);
                std::string coeff_bytes = int128_to_bytes(coeff);
                request.add_string(coeff_bytes);
                request.add_int4(exp);
                break;
            }
            case data::any::index<runtime_t<kind::date>>: {
                auto value = src.to<runtime_t<kind::date>>();
                auto days = static_cast<int32_t>(value.days_since_epoch());
                request.add_int4(days);
                break;
            }
            case data::any::index<runtime_t<kind::time_of_day>>: {
                auto value = src.to<runtime_t<kind::time_of_day>>();
                auto nanos = static_cast<int64_t>(value.time_since_epoch().count());
                request.add_int8(nanos);
                break;
            }
            case data::any::index<runtime_t<kind::time_point>>: {
                auto value = src.to<runtime_t<kind::time_point>>();
                auto offset_seconds = static_cast<int64_t>(value.seconds_since_epoch().count());
                uint32_t nano_adjustment = static_cast<uint32_t>(value.subsecond().count());
                request.add_int8(offset_seconds);
                request.add_uint4(nano_adjustment);
                if(columns[i]->nested() != nullptr) {
                    if(columns[i]->nested()->record_name() == OFFSETDATETIME_RECORD) {
                        request.add_int4(static_cast<int32_t>(global::config_pool()->zone_offset()));
                    }
                }
                break;
            }
            case data::any::index<runtime_t<kind::blob>>: {
                auto value = src.to<runtime_t<kind::blob>>();
                request.add_uint8(1);  // currently input args must be on datastore
                request.add_uint8(value.object_id());
                request.add_uint8(value.lob_reference::reference_tag().value());
                request.add_bool(value.kind() == lob::lob_reference_kind::resolved);
                break;
            }
            case data::any::index<runtime_t<kind::clob>>: {
                auto value = src.to<runtime_t<kind::clob>>();
                request.add_uint8(1);  // currently input args must be on datastore
                request.add_uint8(value.object_id());
                request.add_uint8(value.lob_reference::reference_tag().value());
                request.add_bool(value.kind() == lob::lob_reference_kind::resolved);
                break;
            }
            default:
                // do nothing for unhandled type
                break;
        }
    }
}
std::shared_ptr<const takatori::type::data> determine_return_type(
    plugin::udf::record_descriptor const& output_record,
    std::unordered_map<std::string_view, std::function<std::shared_ptr<const takatori::type::data>()>> const& type_map
) {
    if(! output_record.columns().empty()) {
        auto nest = output_record.columns()[0]->nested();
        if(nest != nullptr) {
            if(auto it = type_map.find(nest->record_name()); it != type_map.end()) { return it->second(); }
        }
        return map_type(output_record.columns()[0]->type_kind());
    }

    return nullptr;
}
std::vector<std::shared_ptr<const takatori::type::data>> build_param_types(
    std::vector<plugin::udf::column_descriptor*> const& pattern,
    std::unordered_map<std::string_view, std::function<std::shared_ptr<const takatori::type::data>()>> const& type_map
) {
    std::vector<std::shared_ptr<const takatori::type::data>> param_types;
    param_types.reserve(pattern.size());

    for(auto* col: pattern) {
        if(! col) continue;

        if(col->type_kind() == plugin::udf::type_kind_type::message) {
            if(auto nested = col->nested()) {
                if(auto it = type_map.find(nested->record_name()); it != type_map.end()) {
                    if(auto ptr = it->second()) { param_types.emplace_back(ptr); }
                }
            }
        } else {
            if(auto mapped = map_type(col->type_kind())) { param_types.emplace_back(mapped); }
        }
    }
    return param_types;
}

std::size_t count_effective_columns(plugin::udf::record_descriptor const& rec) {
    std::size_t total = 0;

    for (auto* col : rec.columns()) {
        if (!col) continue;

        auto* nested = col->nested();
        if (nested) {
            total += count_effective_columns(*nested);
        } else {
            total += 1;
        }
    }
    return total;
}

void register_udf_function_patterns(
    yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo,
    yugawara::function::declaration::definition_id_type& current_id,
    std::function<data::any(evaluator_context&, sequence_view<data::any>)> const& lambda_func,
    plugin::udf::function_descriptor const* fn
) {
    std::string fn_name(fn->function_name());
    std::transform(fn_name.begin(), fn_name.end(), fn_name.begin(), [](unsigned char c) { return std::tolower(c); });
    auto const& input_record = fn->input_record();
    auto const& output_record = fn->output_record();
    auto const& type_map = get_type_map();

    auto return_type = determine_return_type(output_record, type_map);
    if(! return_type) return;

    // (Decimal / Date / LocalTime / LocalDatetime / OffsetDatetime / BlobReference)
    if(auto it = type_map.find(input_record.record_name()); it != type_map.end()) {
        if(auto param_type = it->second()) {
            register_function(functions, repo, current_id, fn_name, return_type, {param_type}, lambda_func);
        }
        return;
    }

    // (argument_patterns)
    for(auto const& pattern: input_record.argument_patterns()) {
        auto param_types = build_param_types(pattern, type_map);
        if(! param_types.empty()) {
            register_function(functions, repo, current_id, fn_name, return_type, param_types, lambda_func);
        }
    }
    if (count_effective_columns(input_record) == 0) {
        register_function(functions, repo, current_id, fn_name, return_type, {}, lambda_func);
    }
}
std::vector<plugin::udf::column_descriptor*> const*
find_matched_pattern(plugin::udf::function_descriptor const* fn, sequence_view<data::any> const& args) {
    auto const& input = fn->input_record();
    auto const& type_map = type_index_map();
    auto const& nested_map = nested_type_map();
    for(auto const& pattern: input.argument_patterns()) {
        if(pattern.size() != args.size()) continue;
        bool match = true;
        for(std::size_t i = 0; i < args.size(); ++i) {
            auto const& arg = args[i];
            auto const& col = pattern[i];
            auto kind = col->type_kind();
            if(kind == plugin::udf::type_kind_type::string || kind == plugin::udf::type_kind_type::message) {
                if(auto nested = col->nested()) {
                    auto nested_it = nested_map.find(nested->record_name());
                    match &= (nested_it != nested_map.end() && arg.type_index() == nested_it->second);
                }
            } else {
                auto it = type_map.find(kind);
                match &= (it != type_map.end() && arg.type_index() == it->second);
            }

            if(! match) break;
        }

        if(match) return &pattern;
    }

    return nullptr;
}

bool build_udf_request(
    plugin::udf::generic_record_impl& request,
    evaluator_context& ctx,
    plugin::udf::function_descriptor const* fn,
    sequence_view<data::any> args
) {
    auto const& record_name = fn->input_record().record_name();
    if(record_name == DECIMAL_RECORD) {
        auto value = args[0].to<runtime_t<kind::decimal>>();
        std::int8_t sign = value.sign();
        std::uint64_t hi = value.coefficient_high();
        std::uint64_t lo = value.coefficient_low();
        std::int32_t exp = value.exponent();

        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        auto ucoeff = (static_cast<unsigned __int128>(hi) << 64) | lo;
        if(sign < 0) ucoeff = -ucoeff;

        std::string bytes(16, '\0');
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        for(int i = 0; i < 16; ++i) { bytes[15 - i] = static_cast<char>((ucoeff >> (i * 8)) & 0xFFU); }

        request.add_string(bytes);
        request.add_int4(exp);
        return true;
    }
    if(record_name == DATE_RECORD) {
        auto value = args[0].to<runtime_t<kind::date>>();
        request.add_int4(static_cast<int32_t>(value.days_since_epoch()));
        return true;
    }
    if(record_name == LOCALTIME_RECORD) {
        auto value = args[0].to<runtime_t<kind::time_of_day>>();
        request.add_int8(static_cast<int64_t>(value.time_since_epoch().count()));
        return true;
    }
    if(record_name == LOCALDATETIME_RECORD) {
        auto value = args[0].to<runtime_t<kind::time_point>>();
        request.add_int8(static_cast<int64_t>(value.seconds_since_epoch().count()));
        request.add_uint4(static_cast<uint32_t>(value.subsecond().count()));
        return true;
    }
    if(record_name == OFFSETDATETIME_RECORD) {
        auto value = args[0].to<runtime_t<kind::time_point>>();
        request.add_int8(static_cast<int64_t>(value.seconds_since_epoch().count()));
        request.add_uint4(static_cast<uint32_t>(value.subsecond().count()));
        request.add_int4(static_cast<int32_t>(global::config_pool()->zone_offset()));
        return true;
    }
    // @see include/jogasaki/lob/blob_reference.h
    if(record_name == BLOB_RECORD) {
        auto value = args[0].to<runtime_t<kind::blob>>();
        request.add_uint8(1);  // currently input args must be on datastore
        request.add_uint8(value.object_id());
        request.add_uint8(value.lob_reference::reference_tag().value());
        request.add_bool(value.kind() == lob::lob_reference_kind::resolved);
        // the ID of the storage where the BLOB data is stored.
        // uint64 storage_id = 1;
        // the ID of the element within the BLOB storage.
        // uint64 object_id = 2;
        // a tag for additional access control.
        // uint64 tag = 3;
        // whether the object is provisioned
        // bool provisioned = 4;
        return true;
    }
    // @see include/jogasaki/lob/clob_reference.h
    if(record_name == CLOB_RECORD) {
        auto value = args[0].to<runtime_t<kind::clob>>();
        request.add_uint8(1);  // currently input args must be on datastore
        request.add_uint8(value.object_id());
        request.add_uint8(value.lob_reference::reference_tag().value());
        request.add_bool(value.kind() == lob::lob_reference_kind::resolved);
        // the ID of the storage where the BLOB data is stored.
        // uint64 storage_id = 1;
        // the ID of the element within the BLOB storage.
        // uint64 object_id = 2;
        // a tag for additional access control.
        // uint64 tag = 3;
        // whether the object is provisioned
        // bool provisioned = 4;
        return true;
    }
    auto const* matched_pattern = find_matched_pattern(fn, args);
    if(! matched_pattern) {
        ctx.add_error({error_kind::invalid_input_value, "No matching argument pattern found for given arguments"});
        return false;
    }
    fill_request_with_args(request, args, *matched_pattern);
    return true;
}

data::any build_decimal_data(std::string const& unscaled, std::int32_t exponent) {
    bool negative = false;
    unsigned __int128 ucoeff = 0;

    bool is_negative = (static_cast<unsigned char>(unscaled[0]) & 0x80U) != 0U;
    if(! unscaled.empty() && is_negative) {
        negative = true;
        std::vector<uint8_t> bytes;
        bytes.reserve(unscaled.size());
        for(char c: unscaled) { bytes.emplace_back(static_cast<uint8_t>(c)); }
        for(auto& b: bytes) b = ~b;
        for(int i = static_cast<int>(bytes.size()) - 1; i >= 0; --i) {
            if(++bytes[i] != 0) break;
        }
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        for(uint8_t b: bytes) ucoeff = (ucoeff << 8) | b;
    } else {
        // NOLINTNEXTLINE(hicpp-signed-bitwise)
        for(uint8_t b: unscaled) ucoeff = (ucoeff << 8) | b;
    }
    // NOLINTNEXTLINE(hicpp-signed-bitwise)
    auto coeff_high = static_cast<std::uint64_t>((ucoeff >> 64) & 0xFFFFFFFFFFFFFFFFULL);
    // NOLINTNEXTLINE(hicpp-signed-bitwise)
    auto coeff_low = static_cast<std::uint64_t>(ucoeff & 0xFFFFFFFFFFFFFFFFULL);
    std::int64_t sign = negative ? -1 : (ucoeff == 0 ? 0 : +1);

    takatori::decimal::triple triple_value(sign, coeff_high, coeff_low, exponent);
    return data::any{std::in_place_type<runtime_t<kind::decimal>>, triple_value};
}

template<typename RuntimeT, typename FetchFn>
void fetch_and_emplace(std::vector<data::any>& result, FetchFn fetch_fn) {
    if(auto v = fetch_fn()) {
        result.emplace_back(std::in_place_type<RuntimeT>, *v);
    } else {
        result.emplace_back();
    }
}

template<typename RuntimeT, typename FetchFn, typename CastFn>
void fetch_and_emplace_cast(std::vector<data::any>& result, FetchFn fetch_fn, CastFn cast_fn) {
    if(auto v = fetch_fn()) {
        result.emplace_back(std::in_place_type<RuntimeT>, cast_fn(*v));
    } else {
        result.emplace_back();
    }
}

std::vector<data::any> cursor_to_any_values(
    plugin::udf::generic_record_impl& response,
    std::vector<plugin::udf::column_descriptor*> const& cols,
    evaluator_context& ctx
) {
    std::vector<data::any> result;

    if(auto cursor = response.cursor()) {
        for(auto const* col: cols) {
            auto type_kind = col->type_kind();

            switch(type_kind) {
                case plugin::udf::type_kind_type::sfixed4:
                case plugin::udf::type_kind_type::int4:
                case plugin::udf::type_kind_type::sint4:
                    fetch_and_emplace<runtime_t<kind::int4>>(result, [&] { return cursor->fetch_int4(); });
                    break;

                case plugin::udf::type_kind_type::sfixed8:
                case plugin::udf::type_kind_type::int8:
                case plugin::udf::type_kind_type::sint8:
                    fetch_and_emplace<runtime_t<kind::int8>>(result, [&] { return cursor->fetch_int8(); });
                    break;

                case plugin::udf::type_kind_type::uint4:
                case plugin::udf::type_kind_type::fixed4:
                    fetch_and_emplace_cast<runtime_t<kind::int4>>(
                        result,
                        [&] { return cursor->fetch_uint4(); },
                        [](auto x) { return static_cast<std::int32_t>(x); }
                    );
                    break;

                case plugin::udf::type_kind_type::uint8:
                case plugin::udf::type_kind_type::fixed8:
                    fetch_and_emplace_cast<runtime_t<kind::int8>>(
                        result,
                        [&] { return cursor->fetch_uint8(); },
                        [](auto x) { return static_cast<std::int64_t>(x); }
                    );
                    break;

                case plugin::udf::type_kind_type::float4:
                    fetch_and_emplace<runtime_t<kind::float4>>(result, [&] { return cursor->fetch_float(); });
                    break;

                case plugin::udf::type_kind_type::float8:
                    fetch_and_emplace<runtime_t<kind::float8>>(result, [&] { return cursor->fetch_double(); });
                    break;

                case plugin::udf::type_kind_type::boolean:
                    fetch_and_emplace<runtime_t<kind::boolean>>(result, [&] { return cursor->fetch_bool(); });
                    break;

                case plugin::udf::type_kind_type::string:
                    if(auto v = cursor->fetch_string()) {
                        result.emplace_back(
                            std::in_place_type<runtime_t<kind::character>>,
                            runtime_t<kind::character>{ctx.resource(), *v}
                        );
                    } else {
                        result.emplace_back();
                    }
                    break;

                case plugin::udf::type_kind_type::bytes:
                    if(auto v = cursor->fetch_string()) {
                        result.emplace_back(
                            std::in_place_type<runtime_t<kind::octet>>,
                            runtime_t<kind::octet>{ctx.resource(), *v}
                        );
                    } else {
                        result.emplace_back();
                    }
                    break;

                case plugin::udf::type_kind_type::group:
                case plugin::udf::type_kind_type::message: {
                    if(auto nested_cols = col->nested()) {
                        auto nested_values = cursor_to_any_values(response, nested_cols->columns(), ctx);
                        result.insert(result.end(), nested_values.begin(), nested_values.end());
                    } else {
                        result.emplace_back();
                    }
                    break;
                }

                default: result.emplace_back(); break;
            }
        }
    }

    return result;
}
data::any build_decimal_response(plugin::udf::generic_record_cursor& cursor) {
    auto unscaled_opt = cursor.fetch_string();
    auto exponent_opt = cursor.fetch_int4();
    if(unscaled_opt && exponent_opt) { return build_decimal_data(*unscaled_opt, *exponent_opt); }
    return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
}
data::any build_date_response(plugin::udf::generic_record_cursor& cursor) {
    if(auto result = cursor.fetch_int4()) {
        takatori::datetime::date tp(static_cast<takatori::datetime::date::difference_type>(*result));
        return data::any{std::in_place_type<runtime_t<kind::date>>, tp};
    }
    return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
}
data::any build_localtime_response(plugin::udf::generic_record_cursor& cursor) {
    if(auto result = cursor.fetch_int8()) {
        takatori::datetime::time_of_day tp(static_cast<takatori::datetime::time_of_day::time_unit>(*result));
        return data::any{std::in_place_type<runtime_t<kind::time_of_day>>, tp};
    }
    return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
}
data::any build_localdatetime_response(plugin::udf::generic_record_cursor& cursor) {
    auto offset_seconds = cursor.fetch_int8();
    auto nano_adjustment = cursor.fetch_uint4();
    if(offset_seconds && nano_adjustment) {
        takatori::datetime::time_point value{
            takatori::datetime::time_point::offset_type(*offset_seconds),
            std::chrono::nanoseconds(*nano_adjustment)};
        return data::any{std::in_place_type<runtime_t<kind::time_point>>, value};
    }
    return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
}
template <class Ref> data::any build_lob_response_impl(plugin::udf::generic_record_cursor& cursor) {
    auto storage_id = cursor.fetch_uint8();
    auto object_id = cursor.fetch_uint8();
    auto tag = cursor.fetch_uint8();
    auto provisioned = cursor.fetch_bool();

    if (! storage_id || ! object_id || ! tag) {
        return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
    }
    if (storage_id.value() == 1ULL) {
        if (provisioned && provisioned.value()) {
            return data::any{std::in_place_type<Ref>,
                Ref{object_id.value(), jogasaki::lob::lob_data_provider::datastore}.reference_tag(tag)};
        }
        return data::any{std::in_place_type<Ref>, Ref{object_id.value()}.reference_tag(tag)};
    }
    if (storage_id.value() == 0ULL) {
        return data::any{std::in_place_type<Ref>,
            Ref{object_id.value(), jogasaki::lob::lob_data_provider::relay_service_session}.reference_tag(tag)};
    }

    return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
}
data::any build_blob_response(plugin::udf::generic_record_cursor& cursor) {
    return build_lob_response_impl<lob::blob_reference>(cursor);
}
data::any build_clob_response(plugin::udf::generic_record_cursor& cursor) {
    return build_lob_response_impl<lob::clob_reference>(cursor);
}
std::unordered_map<std::string_view, data::any (*)(plugin::udf::generic_record_cursor&)> const& response_builder_map() {
    static const std::unordered_map<std::string_view, data::any (*)(plugin::udf::generic_record_cursor&)> map{
        {DECIMAL_RECORD, &build_decimal_response},
        {DATE_RECORD, &build_date_response},
        {LOCALTIME_RECORD, &build_localtime_response},
        {LOCALDATETIME_RECORD, &build_localdatetime_response},
        {OFFSETDATETIME_RECORD, &build_localdatetime_response},
        {BLOB_RECORD, &build_blob_response},
        {CLOB_RECORD, &build_clob_response},
    };
    return map;
}
data::any build_udf_response(
    plugin::udf::generic_record_impl& response,
    evaluator_context& ctx,
    plugin::udf::function_descriptor const* fn
) {
    auto const& output = fn->output_record();
    auto cursor = response.cursor();
    if(! cursor) {
        ctx.add_error({error_kind::unknown, "Response has no cursor"});
        return data::any{std::in_place_type<error>, error(error_kind::unknown)};
    }
    auto nest = output.columns()[0]->nested();
    auto const& map = response_builder_map();

    if(nest != nullptr) {
        if(auto it = map.find(nest->record_name()); it != map.end()) { return it->second(*cursor); }
    }
    auto output_values = cursor_to_any_values(response, output.columns(), ctx);
    if(! output_values.empty()) { return output_values.front(); }
    ctx.add_error({error_kind::invalid_input_value, "Invalid or missing UDF response"});
    return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
}

std::function<data::any(evaluator_context&, sequence_view<data::any>)> make_udf_lambda(
    std::shared_ptr<plugin::udf::generic_client> const& client,
    plugin::udf::function_descriptor const* fn
) {
    return [client, fn](evaluator_context& ctx, sequence_view<data::any> args) -> data::any {
        plugin::udf::generic_record_impl request;
        if(! build_udf_request(request, ctx, fn, args)) {
            return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
        }
        plugin::udf::generic_record_impl response;
        grpc::ClientContext context;
        // TODO: make these metadata configurable
        auto* bs = ctx.blob_session();
        assert_with_exception(bs != nullptr, bs, bs);
        auto session_id = bs->get_or_create()->session_id();
        blob_grpc_metadata metadata{session_id,
            std::string(global::config_pool()->grpc_server_endpoint()),
            global::config_pool()->grpc_server_secure(), "stream", 1024ULL * 1024ULL};
        if (! metadata.apply(context)) {
            ctx.add_error({error_kind::unknown, "Failed to apply gRPC metadata"});
            return data::any{std::in_place_type<error>, error(error_kind::unknown)};
        }

        client->call(context, {0, fn->function_index()}, request, response);

        if(response.error()) {
            ctx.add_error(
                {error_kind::unknown,
                 "RPC failed: code=" + std::string(plugin::udf::to_string_view(response.error()->code())) +
                     ", message=" + std::string(response.error()->message())}
            );
            return data::any{std::in_place_type<error>, error(error_kind::unknown)};
        }
        return build_udf_response(response, ctx, fn);
    };
}

bool check_supported_version(plugin::udf::package_descriptor const& pkg) {
    auto const& v = pkg.version();
    if (is_supported_version(v)) {
        LOG_LP(INFO) << "[gRPC] Package '" << pkg.file_name() << "' version " << v.major() << "."
                     << v.minor() << "." << v.patch();
        return true;
    }
    LOG_LP(WARNING) << "[gRPC] Package '" << pkg.file_name() << "' has unsupported version "
                    << v.major() << "." << v.minor() << "." << v.patch() << ". Only version "
                    << SUPPORTED_MAJOR << "." << SUPPORTED_MINOR << ".x is supported.";

    return false;
}

}  // anonymous namespace

bool blob_grpc_metadata::apply(grpc::ClientContext& ctx) const noexcept {
    ctx.AddMetadata("x-tsurugi-blob-session", std::to_string(session_id_));
    ctx.AddMetadata("x-tsurugi-blob-endpoint", endpoint_);
    ctx.AddMetadata("x-tsurugi-blob-secure", secure_ ? "true" : "false");
    ctx.AddMetadata("x-tsurugi-blob-transport", transport_);
    ctx.AddMetadata("x-tsurugi-blob-stream-chunk-size", std::to_string(chunk_size_));
    return true;
}

void add_udf_scalar_functions(
    ::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo,
    const std::vector<
        std::tuple<std::shared_ptr<plugin::udf::plugin_api>, std::shared_ptr<plugin::udf::generic_client>>>& plugins
) {
    using namespace ::yugawara;
    // @see
    // https://github.com/project-tsurugi/jogasaki/blob/master/docs/internal/sql_functions.md
    yugawara::function::declaration::definition_id_type current_id = 19999;
    for(auto const& tup: plugins) {
        auto client = std::get<1>(tup);
        auto plugin = std::get<0>(tup);
        // plugin::udf::print_plugin_info(plugin);
        auto packages = plugin->packages();
        for(auto const* pkg: packages) {
            if (!check_supported_version(*pkg)) {
                continue;
            }
            for(auto const* svc: pkg->services()) {
                for(auto const* fn: svc->functions()) {
                    auto lambda_func = make_udf_lambda(client, fn);
                    register_udf_function_patterns(functions, repo, current_id, lambda_func, fn);
                }
            }
        }
    }
}

}  // namespace jogasaki::executor::function
