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
#include <boost/algorithm/string.hpp>
#include <boost/assert.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tsl/hopscotch_hash.h>
#include <tsl/hopscotch_set.h>
#include <variant>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
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

namespace {

std::shared_ptr<takatori::type::data const> map_type(plugin::udf::type_kind_type kind) {
    namespace t = takatori::type;
    using K     = plugin::udf::type_kind_type;
    switch (kind) {
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
    if (auto opt = nv.value(); opt) {
        return std::visit(
            [&](auto&& val) -> data::any {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, std::monostate>) {
                    return {};
                } else if constexpr (std::is_same_v<T, bool>) {
                    return data::any{std::in_place_type<runtime_t<kind::boolean>>, val};
                } else if constexpr (std::is_same_v<T, std::int32_t>) {
                    return data::any{std::in_place_type<runtime_t<kind::int4>>, val};
                } else if constexpr (std::is_same_v<T, std::int64_t>) {
                    return data::any{std::in_place_type<runtime_t<kind::int8>>, val};
                } else if constexpr (std::is_same_v<T, std::uint32_t>) {
                    return data::any{std::in_place_type<runtime_t<kind::int4>>, val};
                } else if constexpr (std::is_same_v<T, std::uint64_t>) {
                    return data::any{std::in_place_type<runtime_t<kind::int8>>, val};
                } else if constexpr (std::is_same_v<T, float>) {
                    return data::any{std::in_place_type<runtime_t<kind::float4>>, val};
                } else if constexpr (std::is_same_v<T, double>) {
                    return data::any{std::in_place_type<runtime_t<kind::float8>>, val};
                } else if constexpr (std::is_same_v<T, std::string>) {
                    return data::any{std::in_place_type<runtime_t<kind::character>>,
                        runtime_t<kind::character>{ctx.resource(), val}};
                } else {
                    return {};
                }
            },
            *opt);
    }
    return {};
}

void fill_request_with_args(plugin::udf::generic_record_impl& request,
    sequence_view<data::any> args, const std::vector<plugin::udf::column_descriptor*>& columns) {

    for (std::size_t i = 0; i < columns.size(); ++i) {
        const auto& type = columns[i]->type_kind();
        const auto& src  = args[i];
        plugin::udf::NativeValue val;
        switch (src.type_index()) {
            case data::any::index<runtime_t<kind::int4>>:
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::int4>>(), type};
                break;
            case data::any::index<runtime_t<kind::int8>>:
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::int8>>(), type};
                break;
            case data::any::index<runtime_t<kind::float4>>:
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::float4>>(), type};
                break;
            case data::any::index<runtime_t<kind::float8>>:
                val = plugin::udf::NativeValue{src.to<runtime_t<kind::float8>>(), type};
                break;
            case data::any::index<accessor::binary>: {
                auto bin = src.to<runtime_t<kind::octet>>();
                val      = plugin::udf::NativeValue{static_cast<std::string>(bin), type};
                break;
            }
            case data::any::index<accessor::text>: {
                auto ch = src.to<runtime_t<kind::character>>();
                val     = plugin::udf::NativeValue{static_cast<std::string>(ch), type};
                break;
            }
            default:
                val = plugin::udf::NativeValue{}; // null
                break;
        }

        add_arg_value(request, val);
    }
}

} // anonymous namespace
void add_udf_functions(::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo,
    const std::vector<std::tuple<std::shared_ptr<plugin::udf::plugin_api>,
        std::shared_ptr<plugin::udf::generic_client>>>& plugins) {
    using namespace ::yugawara;
    for (const auto& tup : plugins) {
        auto client = std::get<1>(tup);
        auto plugin = std::get<0>(tup);
        // plugin::udf::print_plugin_info(plugin);
        auto packages = plugin->packages();
        for (const auto* pkg : packages) {
            for (const auto* svc : pkg->services()) {
                for (const auto* fn : svc->functions()) {
                    std::string fn_name(fn->function_name());
                    std::transform(fn_name.begin(), fn_name.end(), fn_name.begin(),
                        [](unsigned char c) { return std::tolower(c); });
                    auto lambda_func = [client, fn](evaluator_context& ctx,
                                           sequence_view<data::any> args) -> data::any {
                        BOOST_ASSERT(args.size() == fn->input_record().columns().size()); // NOLINT
                        plugin::udf::generic_record_impl request;
                        plugin::udf::generic_record_impl response;
                        const auto& input  = fn->input_record();
                        const auto& output = fn->output_record();
                        fill_request_with_args(request, args, input.columns());
                        grpc::ClientContext context;
                        client->call(context, {0, fn->function_index()}, request, response);
                        std::vector<plugin::udf::NativeValue> output_values =
                            cursor_to_native_values(response, output.columns());
                        //  print_native_values(output_values);
                        const auto& output_value = output_values.front();
                        return native_to_any(output_value, ctx);
                    };
                    auto info =
                        std::make_shared<scalar_function_info>(scalar_function_kind::user_defined,
                            lambda_func, fn->input_record().columns().size());
                    // @see
                    // https://github.com/project-tsurugi/jogasaki/blob/master/docs/internal/sql_functions.md
                    auto id = 20000 + fn->function_index();
                    repo.add(id, info);
                    auto return_type = map_type(fn->output_record().columns()[0]->type_kind());
                    std::vector<std::shared_ptr<takatori::type::data const>> param_types;
                    param_types.reserve(fn->input_record().columns().size());
                    for (auto col : fn->input_record().columns()) {
                        param_types.emplace_back(map_type(col->type_kind()));
                    }
                    functions.add(yugawara::function::declaration{
                        id, fn_name, return_type, std::move(param_types)});
                }
            }
        }
    }
}

} // namespace jogasaki::executor::function
