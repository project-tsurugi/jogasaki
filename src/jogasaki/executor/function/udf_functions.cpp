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
#include <takatori/type/character.h>
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
#include <jogasaki/udf/error_info.h>
#include <jogasaki/udf/generic_record_impl.h>
#include <jogasaki/udf/plugin_loader.h>
#include <jogasaki/udf/udf_loader.h>
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

std::shared_ptr<takatori::type::data const> map_type(plugin::udf::type_kind_type kind) {
    namespace t = takatori::type;
    using K = plugin::udf::type_kind_type;
    switch(kind) {
        case K::FLOAT8: return std::make_shared<t::simple_type<t::type_kind::float8>>();
        case K::FLOAT4: return std::make_shared<t::simple_type<t::type_kind::float4>>();
        case K::INT8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        case K::UINT8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        case K::INT4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::FIXED8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        case K::FIXED4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::BOOL: return std::make_shared<t::simple_type<t::type_kind::boolean>>();
        case K::STRING: return std::make_shared<t::character>(t::varying);
        case K::GROUP: return std::make_shared<t::character>(t::varying);
        case K::MESSAGE: return std::make_shared<t::character>(t::varying);
        case K::BYTES: return std::make_shared<t::octet>(t::varying);
        case K::UINT4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::ENUM: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::SFIXED4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::SFIXED8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        case K::SINT4: return std::make_shared<t::simple_type<t::type_kind::int4>>();
        case K::SINT8: return std::make_shared<t::simple_type<t::type_kind::int8>>();
        default: return std::make_shared<t::character>(t::varying);
    }
}
data::any native_to_any(const plugin::udf::NativeValue& nv, evaluator_context& ctx) {
    if(auto opt = nv.value(); opt) {

        return std::visit(
            [&](auto&& val) -> data::any {
                using T = std::decay_t<decltype(val)>;
                if constexpr(std::is_same_v<T, std::monostate>) {
                    return {};
                } else if constexpr(std::is_same_v<T, bool>) {
                    return data::any{std::in_place_type<runtime_t<kind::boolean>>, val};
                } else if constexpr(std::is_same_v<T, std::int32_t>) {
                    return data::any{std::in_place_type<runtime_t<kind::int4>>, val};
                } else if constexpr(std::is_same_v<T, std::int64_t>) {
                    return data::any{std::in_place_type<runtime_t<kind::int8>>, val};
                } else if constexpr(std::is_same_v<T, std::uint32_t>) {
                    return data::any{std::in_place_type<runtime_t<kind::int4>>, val};
                } else if constexpr(std::is_same_v<T, std::uint64_t>) {
                    return data::any{std::in_place_type<runtime_t<kind::int8>>, val};
                } else if constexpr(std::is_same_v<T, float>) {
                    return data::any{std::in_place_type<runtime_t<kind::float4>>, val};
                } else if constexpr(std::is_same_v<T, double>) {
                    return data::any{std::in_place_type<runtime_t<kind::float8>>, val};
                } else if constexpr(std::is_same_v<T, std::string>) {
                    if(nv.kind() == plugin::udf::type_kind_type::BYTES) {
                        return data::any{
                            std::in_place_type<runtime_t<kind::octet>>,
                            runtime_t<kind::octet>{ctx.resource(), val}};
                    }
                    return data::any{
                        std::in_place_type<runtime_t<kind::character>>,
                        runtime_t<kind::character>{ctx.resource(), val}};
                } else {
                    return {};
                }
            },
            *opt
        );
    }
    return {};
}

void fill_request_with_args(
    plugin::udf::generic_record_impl& request,
    sequence_view<data::any> args,
    const std::vector<plugin::udf::column_descriptor*>& columns
) {

    for(std::size_t i = 0; i < columns.size(); ++i) {
        const auto& type = columns[i]->type_kind();
        const auto& src = args[i];
        plugin::udf::NativeValue val;
        switch(src.type_index()) {
            case data::any::index<runtime_t<kind::boolean>>: {
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::boolean>>(), type};
                add_arg_value(request, val);
                break;
            }
            case data::any::index<runtime_t<kind::int4>>:
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::int4>>(), type};
                add_arg_value(request, val);
                break;
            case data::any::index<runtime_t<kind::int8>>:
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::int8>>(), type};
                add_arg_value(request, val);
                break;
            case data::any::index<runtime_t<kind::float4>>:
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::float4>>(), type};
                add_arg_value(request, val);
                break;
            case data::any::index<runtime_t<kind::float8>>:
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::float8>>(), type};
                add_arg_value(request, val);
                break;
            case data::any::index<accessor::binary>: {
                auto bin = src.to<runtime_t<kind::octet>>();
                val = plugin::udf::NativeValue{static_cast<std::string>(bin), type};
                add_arg_value(request, val);
                break;
            }
            case data::any::index<accessor::text>: {
                auto ch = src.to<runtime_t<kind::character>>();
                val = plugin::udf::NativeValue{static_cast<std::string>(ch), type};
                add_arg_value(request, val);
                break;
            }
            case data::any::index<runtime_t<kind::decimal>>: {
                auto value = src.to<runtime_t<kind::decimal>>();
                std::int8_t sign = value.sign();
                std::uint64_t hi = value.coefficient_high();
                std::uint64_t lo = value.coefficient_low();
                std::int32_t exp = value.exponent();

                unsigned __int128 ucoeff = (static_cast<unsigned __int128>(hi) << 64) | lo;

                __int128 coeff = (sign < 0) ? -static_cast<__int128>(ucoeff) : static_cast<__int128>(ucoeff);

                std::string bytes(16, '\0');
                for(int i = 0; i < 16; ++i) { bytes[15 - i] = static_cast<char>((coeff >> (i * 8)) & 0xFF); }
                auto val1 = plugin::udf::NativeValue{bytes, plugin::udf::type_kind_type::BYTES};
                auto val2 = plugin::udf::NativeValue{exp, plugin::udf::type_kind_type::INT4};
                add_arg_value(request, val1);
                add_arg_value(request, val2);
                break;
            }
            case data::any::index<runtime_t<kind::date>>: {
                auto value = src.to<runtime_t<kind::date>>();
                auto days = static_cast<int32_t>(value.days_since_epoch());
                auto val1 = plugin::udf::NativeValue{days, plugin::udf::type_kind_type::INT4};
                add_arg_value(request, val1);
                break;
            }
            case data::any::index<runtime_t<kind::time_of_day>>: {
                auto value = src.to<runtime_t<kind::time_of_day>>();
                auto nanos = static_cast<int64_t>(value.time_since_epoch().count());
                auto val1 = plugin::udf::NativeValue{nanos, plugin::udf::type_kind_type::INT8};
                add_arg_value(request, val1);
                break;
            }
            case data::any::index<runtime_t<kind::time_point>>: {
                auto value = src.to<runtime_t<kind::time_point>>();
                auto offset_seconds = static_cast<int64_t>(value.seconds_since_epoch().count());
                uint32_t nano_adjustment = static_cast<uint32_t>(value.subsecond().count());
                auto val1 = plugin::udf::NativeValue{offset_seconds, plugin::udf::type_kind_type::INT8};
                auto val2 = plugin::udf::NativeValue{nano_adjustment, plugin::udf::type_kind_type::UINT4};
                add_arg_value(request, val1);
                add_arg_value(request, val2);
                break;
            }
            case data::any::index<runtime_t<kind::blob>>: {
                // auto b = src.to<runtime_t<kind::blob>>();
                // val    = plugin::udf::NativeValue{static_cast<takatori::lob::blob_reference>(b),
                // type};
                break;
            }
            case data::any::index<runtime_t<kind::clob>>: {
                // auto c = src.to<runtime_t<kind::clob>>();
                // val    = plugin::udf::NativeValue{static_cast<takatori::lob::clob_reference>(c),
                // type};
                break;
            }
            default:
                val = plugin::udf::NativeValue{};  // null
                add_arg_value(request, val);
                break;
        }
    }
}

void register_udf_function_patterns(
    yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo,
    yugawara::function::declaration::definition_id_type& current_id,
    const std::function<data::any(evaluator_context&, sequence_view<data::any>)>& lambda_func,
    const plugin::udf::function_descriptor* fn
) {
    std::string fn_name(fn->function_name());
    std::transform(fn_name.begin(), fn_name.end(), fn_name.begin(), [](unsigned char c) { return std::tolower(c); });

    const auto& input_record = fn->input_record();
    std::shared_ptr<const takatori::type::data> return_type;
    // @see
    // https://github.com/project-tsurugi/takatori/blob/master/include/takatori/type/type_kind.h
    if(fn->output_record().record_name() == "tsurugidb.udf.value.Decimal") {
        return_type = std::make_shared<takatori::type::decimal>();
    } else if(fn->output_record().record_name() == "tsurugidb.udf.value.Date") {
        return_type = std::make_shared<takatori::type::date>();
    } else if(fn->output_record().record_name() == "tsurugidb.udf.value.LocalTime") {
        return_type = std::make_shared<takatori::type::time_of_day>();
    } else if(fn->output_record().record_name() == "tsurugidb.udf.value.LocalDatetime") {
        return_type = std::make_shared<takatori::type::time_point>();
    } else if(fn->output_record().record_name() == "tsurugidb.udf.value.OffsetDatetime") {
        return_type = std::make_shared<takatori::type::time_point>();
    } else if(fn->output_record().record_name() == "tsurugidb.udf.value.BlobReference") {
        // return_type = std::make_shared<takatori::type::blob>();
    } else if(fn->output_record().record_name() == "tsurugidb.udf.value.ClobReference") {
        // return_type = std::make_shared<takatori::type::clob>();
    } else {
        return_type = map_type(fn->output_record().columns()[0]->type_kind());
    }
    std::string input_record_name = std::string(input_record.record_name());
    if(input_record_name == "tsurugidb.udf.value.Decimal") {
        current_id++;
        std::vector<std::shared_ptr<takatori::type::data const>> param_types;
        param_types.emplace_back(std::make_shared<takatori::type::decimal>());
        auto info = std::make_shared<scalar_function_info>(scalar_function_kind::user_defined, lambda_func, 1);
        repo.add(current_id, info);

        functions.add(yugawara::function::declaration{current_id, fn_name, return_type, std::move(param_types)});
    } else if(input_record_name == "tsurugidb.udf.value.Date") {
        current_id++;
        std::vector<std::shared_ptr<takatori::type::data const>> param_types;
        param_types.emplace_back(std::make_shared<takatori::type::date>());
        auto info = std::make_shared<scalar_function_info>(scalar_function_kind::user_defined, lambda_func, 1);
        repo.add(current_id, info);
        functions.add(yugawara::function::declaration{current_id, fn_name, return_type, std::move(param_types)});
    } else if(input_record_name == "tsurugidb.udf.value.LocalTime") {
        current_id++;
        std::vector<std::shared_ptr<takatori::type::data const>> param_types;
        param_types.emplace_back(std::make_shared<takatori::type::time_of_day>());
        auto info = std::make_shared<scalar_function_info>(scalar_function_kind::user_defined, lambda_func, 1);
        repo.add(current_id, info);
        functions.add(yugawara::function::declaration{current_id, fn_name, return_type, std::move(param_types)});
    } else if(input_record_name == "tsurugidb.udf.value.LocalDatetime") {
        current_id++;
        std::vector<std::shared_ptr<takatori::type::data const>> param_types;
        param_types.emplace_back(std::make_shared<takatori::type::time_point>());
        auto info = std::make_shared<scalar_function_info>(scalar_function_kind::user_defined, lambda_func, 1);
        repo.add(current_id, info);
        functions.add(yugawara::function::declaration{current_id, fn_name, return_type, std::move(param_types)});
    } else if(input_record_name == "tsurugidb.udf.value.OffsetDatetime") {
        current_id++;
        std::vector<std::shared_ptr<takatori::type::data const>> param_types;
        param_types.emplace_back(std::make_shared<takatori::type::datetime_interval>());
        auto info = std::make_shared<scalar_function_info>(scalar_function_kind::user_defined, lambda_func, 1);
        repo.add(current_id, info);
        functions.add(yugawara::function::declaration{current_id, fn_name, return_type, std::move(param_types)});
    } else if(input_record.record_name() == "tsurugidb.udf.value.BlobReference") {
        // Not yet
    } else if(input_record.record_name() == "tsurugidb.udf.value.ClobReference") {
        // Not yet
    } else {
        for(auto const& pattern: input_record.argument_patterns()) {
            current_id++;
            std::vector<std::shared_ptr<takatori::type::data const>> param_types;
            param_types.reserve(pattern.size());
            for(auto col: pattern) {
                if(col->type_kind() == plugin::udf::type_kind_type::MESSAGE) {
                    auto nested = col->nested();
                    if(nested) {
                        std::string nested_name = std::string(nested->record_name());
                        if(nested_name == "tsurugidb.udf.value.Decimal") {
                            param_types.emplace_back(std::make_shared<takatori::type::decimal>());
                        } else if(nested_name == "tsurugidb.udf.value.Date") {
                            param_types.emplace_back(std::make_shared<takatori::type::date>());
                        } else if(nested_name == "tsurugidb.udf.value.LocalTime") {
                            param_types.emplace_back(std::make_shared<takatori::type::time_of_day>());
                        } else if(nested_name == "tsurugidb.udf.value.LocalDatetime") {
                            param_types.emplace_back(std::make_shared<takatori::type::time_point>());
                        } else if(nested_name == "tsurugidb.udf.value.OffsetDatetime") {
                            param_types.emplace_back(std::make_shared<takatori::type::datetime_interval>());
                        } else if(nested_name == "tsurugidb.udf.value.BlobReference") {
                            // param_types.emplace_back(std::make_shared<takatori::type::blob>());
                        } else if(nested_name == "tsurugidb.udf.value.ClobReference") {
                            // param_types.emplace_back(std::make_shared<takatori::type::clob>());
                        }
                    }
                } else {
                    param_types.emplace_back(map_type(col->type_kind()));
                }
            }
            auto info =
                std::make_shared<scalar_function_info>(scalar_function_kind::user_defined, lambda_func, pattern.size());
            repo.add(current_id, info);
            functions.add(yugawara::function::declaration{current_id, fn_name, return_type, std::move(param_types)});
        }
    }
}
const std::vector<plugin::udf::column_descriptor*>*
find_matched_pattern(const plugin::udf::function_descriptor* fn, const sequence_view<data::any>& args) {
    const auto& input = fn->input_record();

    for(auto const& pattern: input.argument_patterns()) {
        if(pattern.size() != args.size()) continue;

        bool match = true;
        for(std::size_t i = 0; i < args.size(); ++i) {
            const auto& arg = args[i];
            const auto& col = pattern[i];
            auto kind = col->type_kind();
            switch(kind) {
                case plugin::udf::type_kind_type::FLOAT8:
                    match &= (arg.type_index() == data::any::index<runtime_t<kind::float8>>);
                    break;
                case plugin::udf::type_kind_type::FLOAT4:
                    match &= (arg.type_index() == data::any::index<runtime_t<kind::float4>>);
                    break;

                case plugin::udf::type_kind_type::INT8:
                case plugin::udf::type_kind_type::SINT8:
                case plugin::udf::type_kind_type::SFIXED8:
                case plugin::udf::type_kind_type::UINT8:
                case plugin::udf::type_kind_type::FIXED8:
                    match &= (arg.type_index() == data::any::index<runtime_t<kind::int8>>);
                    break;

                case plugin::udf::type_kind_type::INT4:
                case plugin::udf::type_kind_type::SINT4:
                case plugin::udf::type_kind_type::SFIXED4:
                case plugin::udf::type_kind_type::UINT4:
                case plugin::udf::type_kind_type::FIXED4:
                    match &= (arg.type_index() == data::any::index<runtime_t<kind::int4>>);
                    break;

                case plugin::udf::type_kind_type::BOOL:
                    match &= (arg.type_index() == data::any::index<runtime_t<kind::boolean>>);
                    break;

                case plugin::udf::type_kind_type::STRING:
                case plugin::udf::type_kind_type::MESSAGE: {
                    auto nested = col->nested();
                    if(nested) {
                        std::string nested_name = std::string(nested->record_name());
                        if(nested_name == "tsurugidb.udf.value.Decimal") {
                            match &= (arg.type_index() == data::any::index<runtime_t<kind::decimal>>);
                        } else if(nested_name == "tsurugidb.udf.value.Date") {
                            match &= (arg.type_index() == data::any::index<runtime_t<kind::date>>);
                        } else if(nested_name == "tsurugidb.udf.value.LocalTime") {
                            match &= (arg.type_index() == data::any::index<runtime_t<kind::time_of_day>>);
                        } else if(nested_name == "tsurugidb.udf.value.LocalDatetime") {
                            match &= (arg.type_index() == data::any::index<runtime_t<kind::time_point>>);
                        } else if(nested_name == "tsurugidb.udf.value.OffsetDatetime") {
                            match &= (arg.type_index() == data::any::index<runtime_t<kind::time_point>>);
                        } else if(nested_name == "tsurugidb.udf.value.BlobReference") {
                            // match &= (arg.type_index() ==
                            // data::any::index<runtime_t<kind::blob>>);
                        } else if(nested_name == "tsurugidb.udf.value.ClobReference") {
                            // match &= (arg.type_index() ==
                            // data::any::index<runtime_t<kind::clob>>);
                        }
                    }
                    break;
                }
                case plugin::udf::type_kind_type::GROUP:
                    match &= (arg.type_index() == data::any::index<accessor::text>);
                    break;

                case plugin::udf::type_kind_type::BYTES:
                    match &= (arg.type_index() == data::any::index<accessor::binary>);
                    break;

                default: match = false; break;
            }

            if(! match) break;
        }

        if(match) { return &pattern; }
    }

    return nullptr;
}
std::function<data::any(evaluator_context&, sequence_view<data::any>)> make_udf_lambda(
    const std::shared_ptr<plugin::udf::generic_client>& client,
    const plugin::udf::function_descriptor* fn
) {
    return [client, fn](evaluator_context& ctx, sequence_view<data::any> args) -> data::any {
        plugin::udf::generic_record_impl request;
        if(fn->input_record().record_name() == "tsurugidb.udf.value.Decimal") {
            auto value = args[0].to<runtime_t<kind::decimal>>();
            std::int8_t sign = value.sign();
            std::uint64_t hi = value.coefficient_high();
            std::uint64_t lo = value.coefficient_low();
            std::int32_t exp = value.exponent();

            __int128 coeff = (static_cast<__int128>(hi) << 64) | lo;
            if(sign < 0) coeff = -coeff;

            std::string bytes(16, '\0');
            for(int i = 0; i < 16; ++i) { bytes[15 - i] = static_cast<char>((coeff >> (i * 8)) & 0xFF); }
            request.add_string(bytes);
            request.add_int4(exp);
        } else if(fn->input_record().record_name() == "tsurugidb.udf.value.Date") {
            auto value = args[0].to<runtime_t<kind::date>>();
            auto days = static_cast<int32_t>(value.days_since_epoch());
            request.add_int4(days);
        } else if(fn->input_record().record_name() == "tsurugidb.udf.value.LocalTime") {
            auto value = args[0].to<runtime_t<kind::time_of_day>>();
            auto nanos = static_cast<int64_t>(value.time_since_epoch().count());
            request.add_int8(nanos);
        } else if(fn->input_record().record_name() == "tsurugidb.udf.value.LocalDatetime") {
            auto value = args[0].to<runtime_t<kind::time_point>>();
            auto offset_seconds = static_cast<int64_t>(value.seconds_since_epoch().count());
            auto nano_adjustment = static_cast<uint32_t>(value.subsecond().count());

            request.add_int8(offset_seconds);
            request.add_uint4(nano_adjustment);
        } else if(fn->input_record().record_name() == "tsurugidb.udf.value.OffsetDatetime") {
            auto value = args[0].to<runtime_t<kind::time_point>>();
            auto offset_seconds = static_cast<int64_t>(value.seconds_since_epoch().count());
            auto nano_adjustment = static_cast<uint32_t>(value.subsecond().count());

            request.add_int8(offset_seconds);
            request.add_uint4(nano_adjustment);
            // request.add_int4(33);
            /*
            message OffsetDatetime {
              sint64 offset_seconds = 1;   // UTC epoch seconds
              uint32 nano_adjustment = 2;  // nanos [0, 10^9)
              sint32 time_zone_offset = 3; // minutes from UTC
            }
            */
        } else if(fn->input_record().record_name() == "tsurugidb.udf.value.BlobReference") {
            // Not yet
        } else if(fn->input_record().record_name() == "tsurugidb.udf.value.ClobReference") {
            // Not yet
        } else {
            const auto* matched_pattern = find_matched_pattern(fn, args);
            if(! matched_pattern) {
                ctx.add_error(
                    {error_kind::invalid_input_value, "No matching argument pattern found for given arguments"}
                );
                return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
            }
            fill_request_with_args(request, args, *matched_pattern);
        }
        plugin::udf::generic_record_impl response;
        grpc::ClientContext context;
        client->call(context, {0, fn->function_index()}, request, response);

        if(response.error()) {
            ctx.add_error(
                {error_kind::unknown,
                 "RPC failed: code=" + response.error()->code_string() +
                     ", message=" + std::string(response.error()->message())}
            );
            return data::any{std::in_place_type<error>, error(error_kind::unknown)};
        }

        const auto& output = fn->output_record();
        if(output.record_name() == "tsurugidb.udf.value.Decimal") {
            if(auto cursor = response.cursor()) {
                std::optional<std::string> unscaled_opt;
                std::optional<std::int32_t> exponent_opt;

                if(auto result = cursor->fetch_string()) { unscaled_opt = *result; }
                if(auto result = cursor->fetch_int4()) { exponent_opt = *result; }
                if(unscaled_opt && exponent_opt) {
                    const std::string& unscaled = *unscaled_opt;
                    std::int32_t exponent = *exponent_opt;

                    unsigned __int128 coefficient = 0;
                    bool negative = false;

                    if(! unscaled.empty() && (static_cast<unsigned char>(unscaled[0]) & 0x80u)) {
                        negative = true;
                        std::vector<uint8_t> bytes(unscaled.begin(), unscaled.end());
                        for(auto& b: bytes) b = ~b;
                        for(int i = static_cast<int>(bytes.size()) - 1; i >= 0; --i) {
                            if(++bytes[i] != 0) break;
                        }
                        for(uint8_t b: bytes) { coefficient = (coefficient << 8) | b; }
                    } else {
                        for(uint8_t b: unscaled) { coefficient = (coefficient << 8) | b; }
                    }

                    // make an unsigned local alias (optional, clearer intent)
                    unsigned __int128 ucoeff = coefficient;

                    auto coeff_high = static_cast<std::uint64_t>((ucoeff >> 64) & 0xFFFFFFFFFFFFFFFFULL);
                    auto coeff_low = static_cast<std::uint64_t>(ucoeff & 0xFFFFFFFFFFFFFFFFULL);
                    std::int64_t sign = negative ? -1 : (ucoeff == 0 ? 0 : +1);

                    takatori::decimal::triple triple_value(sign, coeff_high, coeff_low, exponent);
                    return data::any{std::in_place_type<runtime_t<kind::decimal>>, triple_value};
                }
            }
        } else if(output.record_name() == "tsurugidb.udf.value.Date") {
            takatori::datetime::date tp{};
            if(auto cursor = response.cursor()) {
                if(auto result = cursor->fetch_int4()) {
                    tp = takatori::datetime::date(static_cast<takatori::datetime::date::difference_type>(*result));
                }
            }
            return data::any{std::in_place_type<runtime_t<kind::date>>, tp};
        } else if(output.record_name() == "tsurugidb.udf.value.LocalTime") {
            takatori::datetime::time_of_day tp{};
            if(auto cursor = response.cursor()) {
                if(auto result = cursor->fetch_int8()) {
                    tp = takatori::datetime::time_of_day(static_cast<takatori::datetime::time_of_day::time_unit>(*result
                    ));
                }
            }
            return data::any{std::in_place_type<runtime_t<kind::time_of_day>>, tp};
        } else if(output.record_name() == "tsurugidb.udf.value.LocalDatetime") {
            int64_t offset_seconds{0};
            uint32_t nano_adjustment{0};
            if(auto cursor = response.cursor()) {
                if(auto result = cursor->fetch_int8()) { offset_seconds = *result; }
                if(auto result = cursor->fetch_uint4()) { nano_adjustment = *result; }
            }
            takatori::datetime::time_point value{
                takatori::datetime::time_point::offset_type(offset_seconds),
                std::chrono::nanoseconds(nano_adjustment)};
            return data::any{std::in_place_type<runtime_t<kind::time_point>>, value};
        } else if(output.record_name() == "tsurugidb.udf.value.OffsetDatetime") {
        } else if(output.record_name() == "tsurugidb.udf.value.BlobReference") {
            // Not yet
        } else if(output.record_name() == "tsurugidb.udf.value.ClobReference") {
            // Not yet
        } else {
            std::vector<plugin::udf::NativeValue> output_values = cursor_to_native_values(response, output.columns());
            const auto& output_value = output_values.front();
            return native_to_any(output_value, ctx);
        }
        std::abort();
    };
}

}  // anonymous namespace
void add_udf_functions(
    ::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo,
    const std::vector<
        std::tuple<std::shared_ptr<plugin::udf::plugin_api>, std::shared_ptr<plugin::udf::generic_client>>>& plugins
) {
    using namespace ::yugawara;
    // @see
    // https://github.com/project-tsurugi/jogasaki/blob/master/docs/internal/sql_functions.md
    yugawara::function::declaration::definition_id_type current_id = 19999;
    for(const auto& tup: plugins) {
        auto client = std::get<1>(tup);
        auto plugin = std::get<0>(tup);
        // plugin::udf::print_plugin_info(plugin);
        auto packages = plugin->packages();
        for(const auto* pkg: packages) {
            for(const auto* svc: pkg->services()) {
                for(const auto* fn: svc->functions()) {
                    auto lambda_func = make_udf_lambda(client, fn);
                    register_udf_function_patterns(functions, repo, current_id, lambda_func, fn);
                }
            }
        }
    }
}

}  // namespace jogasaki::executor::function
