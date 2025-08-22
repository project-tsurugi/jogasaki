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
#include "builtin_scalar_functions.h"

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
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/round.h>
#include <jogasaki/utils/base64_utils.h>
#include <jogasaki/utils/string_utils.h>
namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using executor::expr::evaluator_context;
using jogasaki::executor::expr::error_kind;
using jogasaki::executor::expr::error;

using kind = meta::field_type_kind;

void add_builtin_scalar_functions(
    ::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo
) {
    namespace t = takatori::type;
    using namespace ::yugawara;

    /////////
    // octet_length
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::octet_length,
            builtin::octet_length,
            1
        );
        auto name = "octet_length";
        auto id = scalar_function_id::id_11001;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::character(t::varying),
            },
        });
        id = scalar_function_id::id_11001;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::octet(t::varying),
            },
        });
    }

    /////////
    // current_date
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::current_date,
            builtin::current_date,
            0
        );
        auto name = "current_date";
        auto id = scalar_function_id::id_11002;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::date(),
            {},
        });
    }
    /////////
    // localtime
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::localtime,
            builtin::localtime,
            0
        );
        auto name = "localtime";
        auto id = scalar_function_id::id_11003;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::time_of_day(),
            {},
        });
    }
    /////////
    // current_timestamp
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::current_timestamp,
            builtin::current_timestamp,
            0
        );
        auto name = "current_timestamp";
        auto id = scalar_function_id::id_11004;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::time_point(t::with_time_zone),
            {},
        });
    }
    /////////
    // localtimestamp
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::localtimestamp,
            builtin::localtimestamp,
            0
        );
        auto name = "localtimestamp";
        auto id = scalar_function_id::id_11005;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::time_point(),
            {},
        });
    }
    /////////
    // substring
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substring, builtin::substring, 3);
        auto name = "substring";
        auto id   = scalar_function_id::id_11006;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {t::character(t::varying), t::int8(), t::int8()},
        });
        info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substring, builtin::substring, 2);
        id = scalar_function_id::id_11007;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {t::character(t::varying), t::int8()},
        });
        info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substring, builtin::substring, 3);
        id = scalar_function_id::id_11008;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::octet(t::varying),
            {t::octet(t::varying), t::int8(), t::int8()},
        });
        info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substring, builtin::substring, 2);
        id = scalar_function_id::id_11009;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::octet(t::varying),
            {t::octet(t::varying), t::int8()},
        });
    }
    /////////
    // upper
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::upper,
            builtin::upper,
            1
        );
        auto name = "upper";
        auto id = scalar_function_id::id_11010;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {t::character(t::varying)},
        });
    }
    /////////
    // lower
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::lower,
            builtin::lower,
            1
        );
        auto name = "lower";
        auto id = scalar_function_id::id_11011;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {t::character(t::varying)},
        });
    }
    /////////
    // character_length
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::character_length,
            builtin::character_length,
            1
        );
        auto name = "character_length";
        auto id = scalar_function_id::id_11012;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {t::character(t::varying)},
        });
    }
    /////////
    // char_length
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::char_length,
            builtin::character_length,
            1
        );
        auto name = "char_length";
        auto id = scalar_function_id::id_11013;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {t::character(t::varying)},
        });
    }
    /////////
    // abs
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::abs,
            builtin::abs,
            1
        );
        auto name = "abs";
        auto id = scalar_function_id::id_11014;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int4(),
            {t::int4()},
        });
        id = scalar_function_id::id_11015;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {t::int8()},
        });
        id = scalar_function_id::id_11016;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float4(),
            {t::float4()},
        });
        id = scalar_function_id::id_11017;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float8(),
            {t::float8()},
        });
        id = scalar_function_id::id_11018;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {t::decimal()},
        });
    }
    /////////
    // position
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::position, builtin::position, 2);
        auto name = "position";
        auto id   = scalar_function_id::id_11019;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::character(t::varying),
                t::character(t::varying),
            },
        });
    }
    /////////
    // mod
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::mod, builtin::mod, 2);
        auto name = "mod";
        auto id   = scalar_function_id::id_11020;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int4(),
            {
                t::int4(),
                t::int4()
            },
        });
        id   = scalar_function_id::id_11021;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::int4(),
                t::int8()
            },
        });
        id   = scalar_function_id::id_11022;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::int8(),
                t::int4()
            },
        });
        id   = scalar_function_id::id_11023;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::int8(),
                t::int8()
            },
        });
        //
        id   = scalar_function_id::id_11024;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {
                t::int4(),
                t::decimal()
            },
        });
        id   = scalar_function_id::id_11025;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {
                t::decimal(),
                t::int4()
            },
        });
        id   = scalar_function_id::id_11026;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {
                t::decimal(),
                t::int8()
            },
        });
        id   = scalar_function_id::id_11027;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {
                t::int8(),
                t::decimal()
            },
        });
        id   = scalar_function_id::id_11028;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {
                t::decimal(),
                t::decimal()
            },
        });
    }
    /////////
    // substr
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substr, builtin::substring, 3);
        auto name = "substr";
        auto id   = scalar_function_id::id_11029;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {
                t::character(t::varying),
                t::int8(),
                t::int8()
            },
        });
        info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substr, builtin::substring, 2);
        id = scalar_function_id::id_11030;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {
                t::character(t::varying),
                t::int8()
            },
        });
        info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substr, builtin::substring, 3);
        id = scalar_function_id::id_11031;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::octet(t::varying),
            {
                t::octet(t::varying),
                t::int8(),
                t::int8()
            },
        });
        info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substr, builtin::substring, 2);
        id = scalar_function_id::id_11032;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::octet(t::varying),
            {
                t::octet(t::varying),
                t::int8()
            },
        });
    }
    /////////
    // ceil
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::ceil,
            builtin::ceil,
            1
        );
        auto name = "ceil";
        auto id = scalar_function_id::id_11033;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int4(),
            {t::int4()},
        });
        id = scalar_function_id::id_11034;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {t::int8()},
        });
        id = scalar_function_id::id_11035;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float4(),
            {t::float4()},
        });
        id = scalar_function_id::id_11036;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float8(),
            {t::float8()},
        });
        id = scalar_function_id::id_11037;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {t::decimal()},
        });
    }
    /////////
    // floor
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::floor,
            builtin::floor,
            1
        );
        auto name = "floor";
        auto id = scalar_function_id::id_11038;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int4(),
            {t::int4()},
        });
        id = scalar_function_id::id_11039;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {t::int8()},
        });
        id = scalar_function_id::id_11040;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float4(),
            {t::float4()},
        });
        id = scalar_function_id::id_11041;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float8(),
            {t::float8()},
        });
        id = scalar_function_id::id_11042;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {t::decimal()},
        });
    }
    /////////
    // round
    /////////
    {
        auto info =
            std::make_shared<scalar_function_info>(scalar_function_kind::round, builtin::round, 1);
        auto name = "round";
        auto id   = scalar_function_id::id_11043;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int4(),
            {t::int4()},
        });
        id = scalar_function_id::id_11044;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {t::int8()},
        });
        id = scalar_function_id::id_11045;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float4(),
            {t::float4()},
        });
        id = scalar_function_id::id_11046;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float8(),
            {t::float8()},
        });
        id = scalar_function_id::id_11047;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {t::decimal()},
        });

        info =
            std::make_shared<scalar_function_info>(scalar_function_kind::round, builtin::round, 2);
        id = scalar_function_id::id_11048;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int4(),
            {t::int4(), t::int4()},
        });
        id = scalar_function_id::id_11049;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {t::int8(), t::int4()},
        });
        id = scalar_function_id::id_11050;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float4(),
            {t::float4(), t::int4()},
        });
        id = scalar_function_id::id_11051;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float8(),
            {t::float8(), t::int4()},
        });
        id = scalar_function_id::id_11052;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {t::decimal(), t::int4()},
        });

        id = scalar_function_id::id_11053;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int4(),
            {t::int4(), t::int8()},
        });
        id = scalar_function_id::id_11054;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::int8(),
            {t::int8(), t::int8()},
        });
        id = scalar_function_id::id_11055;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float4(),
            {t::float4(), t::int8()},
        });
        id = scalar_function_id::id_11056;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::float8(),
            {t::float8(), t::int8()},
        });
        id = scalar_function_id::id_11057;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::decimal(),
            {t::decimal(), t::int8()},
        });
    }
    /////////
    // encode
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::encode,
            builtin::encode,
            2
        );
        auto name = "encode";
        auto id = scalar_function_id::id_11058;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {t::octet(t::varying),t::character(t::varying)},
        });
    }
    /////////
    // decode
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::decode,
            builtin::decode,
            2
        );
        auto name = "decode";
        auto id = scalar_function_id::id_11059;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::octet(t::varying),
            {t::character(t::varying),t::character(t::varying)},
        });
    }
    /////////
    // rtrim
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::rtrim,
            builtin::rtrim,
            1
        );
        auto name = "rtrim";
        auto id = scalar_function_id::id_11060;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {t::character(t::varying)},
        });
    }
    /////////
    // ltrim
    /////////
    {
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::ltrim,
            builtin::ltrim,
            1
        );
        auto name = "ltrim";
        auto id = scalar_function_id::id_11061;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {t::character(t::varying)},
        });
    }
}

namespace builtin {

data::any octet_length(
    evaluator_context&,
    sequence_view<data::any> args
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    auto& src = static_cast<data::any&>(args[0]);
    if(src.empty()) {
        return {};
    }
    BOOST_ASSERT(
        (src.type_index() == data::any::index<accessor::text> || src.type_index() == data::any::index<accessor::binary>
    ));  //NOLINT
    if(src.type_index() == data::any::index<accessor::binary>) {
        auto bin = src.to<runtime_t<kind::octet>>();
        return data::any{std::in_place_type<runtime_t<kind::int8>>, bin.size()};
    }
    if(src.type_index() == data::any::index<accessor::text>) {
        auto text = src.to<runtime_t<kind::character>>();
        return data::any{std::in_place_type<runtime_t<kind::int8>>, text.size()};
    }
    std::abort();
}

static data::any tx_ts_is_available(evaluator_context& ctx) {
    if(! ctx.transaction()) {
        // programming error
        ctx.add_error({error_kind::unknown, "missing transaction context"});
        return data::any{std::in_place_type<error>, error(error_kind::unknown)};
    }
    if(! ctx.transaction()->start_time().has_value()) {
        ctx.add_error({error_kind::unknown, "no tx begin time was recorded"});
        return data::any{std::in_place_type<error>, error(error_kind::unknown)};
    }
    return {};
}

data::any current_date(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    (void)args;
    if(auto a = tx_ts_is_available(ctx); a.error()) {
        return a;
    }
    takatori::datetime::time_point tp{ctx.transaction()->start_time().value()};
    // Contrary to the name `current_date`, it returns the date part of the local timestamp.
    // But system clock returns chrono::time_point in UTC, so we need to convert it to local timestamp.
    auto os = global::config_pool()->zone_offset();
    tp += std::chrono::minutes{os};
    return data::any{std::in_place_type<runtime_t<kind::date>>, tp.date()};
}

data::any localtime(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    (void)args;
    if(auto a = tx_ts_is_available(ctx); a.error()) {
        return a;
    }
    // This func. returns the time part of the local timestamp.
    // But system clock returns chrono::time_point in UTC, so we need to convert it to local timestamp.
    takatori::datetime::time_point tp{ctx.transaction()->start_time().value()};
    auto os = global::config_pool()->zone_offset();
    tp += std::chrono::minutes{os};
    return data::any{std::in_place_type<runtime_t<kind::time_of_day>>, tp.time()};
}

data::any current_timestamp(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    // same as localtimestamp
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    (void)args;
    if(auto a = tx_ts_is_available(ctx); a.error()) {
        return a;
    }
    takatori::datetime::time_point tp{ctx.transaction()->start_time().value()};
    return data::any{std::in_place_type<runtime_t<kind::time_point>>, tp};
}

data::any localtimestamp(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    // same as current_timestamp
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    (void)args;
    if(auto a = tx_ts_is_available(ctx); a.error()) {
        return a;
    }
    // This func. returns the the local timestamp.
    // But system clock returns chrono::time_point in UTC, so we need to convert it to local timestamp.
    takatori::datetime::time_point tp{ctx.transaction()->start_time().value()};
    auto os = global::config_pool()->zone_offset();
    tp += std::chrono::minutes{os};
    return data::any{std::in_place_type<runtime_t<kind::time_point>>, tp};
}

namespace impl {
enum class encoding_type { ASCII_1BYTE, UTF8_2BYTE, UTF8_3BYTE, UTF8_4BYTE, INVALID };
//
//  mizugaki/src/mizugaki/parser/sql_scanner.ll
//  ASCII   [\x00-\x7f]
//  UTF8_2  [\xc2-\xdf]
//  UTF8_3  [\xe0-\xef]
//  UTF8_4  [\xf0-\xf4]
//  U       [\x80-\xbf]
//

static bool is_continuation_byte(unsigned char c) { return (c & 0xC0U) == 0x80U; }
static encoding_type detect_next_encoding(std::string_view view, const size_t offset) {
    if (view.empty()) return encoding_type::INVALID;
    if (offset >= view.size()) return encoding_type::INVALID;
    const auto offset_2nd = offset + 1;
    const auto offset_3rd = offset + 2;
    auto first            = static_cast<unsigned char>(view[offset]);
    if (first <= 0x7FU) { return encoding_type::ASCII_1BYTE; }
    if (first >= 0xC2U && first <= 0xDFU) {
        return (view.size() >= 2 && is_continuation_byte(view[offset_2nd]))
                   ? encoding_type::UTF8_2BYTE
                   : encoding_type::INVALID;
    }
    if (first >= 0xE0U && first <= 0xEFU) {
        return (view.size() >= 3 && is_continuation_byte(view[offset_2nd]) &&
                   is_continuation_byte(view[offset_3rd]))
                   ? encoding_type::UTF8_3BYTE
                   : encoding_type::INVALID;
    }
    if (first >= 0xF0U && first <= 0xF4U) {
        const auto offset_4th = offset + 3;
        return (view.size() >= 4 && is_continuation_byte(view[offset_2nd]) &&
                   is_continuation_byte(view[offset_3rd]) && is_continuation_byte(view[offset_4th]))
                   ? encoding_type::UTF8_4BYTE
                   : encoding_type::INVALID;
    }
    return encoding_type::INVALID;
}
static size_t get_byte(encoding_type e) {
    switch (e) {
        case encoding_type::ASCII_1BYTE: return 1;
        case encoding_type::UTF8_2BYTE: return 2;
        case encoding_type::UTF8_3BYTE: return 3;
        case encoding_type::UTF8_4BYTE: return 4;
        case encoding_type::INVALID: return 0;
    }
    return 0;
}
static size_t get_start_index_byte(
    std::string_view view, const int64_t zero_based_start, bool is_character_type) {
    if (!is_character_type) { return zero_based_start; }
    size_t tmp_byte = 0;
    size_t offset   = 0;
    for (int64_t i = 0; i < zero_based_start; ++i) {
        tmp_byte = get_byte(detect_next_encoding(view, offset));
        offset += tmp_byte;
    }
    // offset is sum of tmp_bytes
    return offset;
}
static size_t get_size_byte(std::string_view view, const size_t start_byte, const size_t letter_count,
    bool is_character_type) {
    if (!is_character_type) { return letter_count; }
    size_t tmp_byte = 0;
    size_t offset   = start_byte;
    for (size_t i = 0; i < letter_count; ++i) {
        tmp_byte = get_byte(detect_next_encoding(view, offset));
        offset += tmp_byte;
    }
    // offset is sum of tmp_bytes
    return offset - start_byte;
}
static bool is_valid_utf8(std::string_view view) {
    size_t offset = 0;
    while (offset < view.size()) {
        size_t char_size = get_byte(detect_next_encoding(view, offset));
        if (char_size == 0) { return false; }
        offset += char_size;
    }
    return true;
}

static std::optional<size_t> get_utf8_length(std::string_view view) {
    size_t offset = 0;
    size_t count  = 0;
    while (offset < view.size()) {
        size_t char_size = get_byte(detect_next_encoding(view, offset));
        if (char_size == 0) { return std::nullopt; }
        offset += char_size;
        count++;
    }
    return count;
}

template <typename TypeTag>
static data::any extract_substring(std::string_view view, TypeTag type_tag, int64_t zero_based_start,
    std::optional<runtime_t<kind::int8>> casted_length) {
    constexpr bool is_character_type =
        std::is_same_v<std::decay_t<TypeTag>, std::in_place_type_t<runtime_t<kind::character>>>;
    const auto view_size = view.size();
    if (view_size == 0 || zero_based_start < 0 ||
        static_cast<std::size_t>(zero_based_start) >= view_size) {
        return {};
    }
    const auto start_byte = impl::get_start_index_byte(view, zero_based_start, is_character_type);
    if (start_byte >= view_size) { return {}; }
    std::string_view sub_view;
    if (casted_length) {
        const auto casted_length_value = casted_length.value();
        if (casted_length_value == 0) { return data::any{type_tag, ""}; }
        if (casted_length_value > 0) {
            const auto substr_length_byte =
                impl::get_size_byte(view, start_byte, casted_length_value, is_character_type);
            if (view_size < substr_length_byte + start_byte) {
                sub_view = view.substr(start_byte, view_size - start_byte);
            } else {
                sub_view = view.substr(start_byte, substr_length_byte);
            }
        } else {
            return {};
        }
    } else {
        sub_view = view.substr(start_byte);
    }
    return data::any{type_tag, sub_view};
}
template <typename T, typename Func>
static data::any convert_case(evaluator_context& ctx, const data::any& src, Func case_converter) {
    auto text = src.to<T>();
    T v{ctx.resource(), text};
    auto str = static_cast<std::string_view>(v);

    // NOLINTNEXTLINE(modernize-loop-convert)
    for (size_t i = 0; i < str.size(); ++i) {
        auto& c = const_cast<char&>(str[i]);
        if (static_cast<unsigned char>(c) < 0x80) { c = case_converter(c); }
    }
    return data::any{std::in_place_type<T>, v};
}

static int32_t abs_val(int32_t x) {
    int32_t mask = x >> 31;   // NOLINT(hicpp-signed-bitwise)
    return (x + mask) ^ mask; // NOLINT(hicpp-signed-bitwise)
}

static int64_t abs_val(int64_t x) {
    int64_t mask = x >> 63;   // NOLINT(hicpp-signed-bitwise)
    return (x + mask) ^ mask; // NOLINT(hicpp-signed-bitwise)
}

static size_t count_utf8_chars(std::string_view str, size_t i) {
    size_t offset     = 0;
    size_t char_count = 1;
    while (offset < i) {
        char_count++;
        size_t char_size = get_byte(detect_next_encoding(str, offset));
        if (offset + char_size >= i) break;
        offset += char_size;
    }
    return char_count;
}
static data::any extract_position(std::string_view substr, std::string_view str) {
    const std::size_t str_size    = str.size();
    const std::size_t substr_size = substr.size();
    std::size_t pos               = std::string_view::npos;
    for (std::size_t i = 0; i + substr_size <= str_size; ++i) {
        if (std::memcmp(str.data() + i, substr.data(), substr_size) == 0) {
            pos = i;
            break;
        }
    }
    if (pos == std::string_view::npos) {
        return data::any{std::in_place_type<runtime_t<kind::int8>>, 0};
    }
    const std::size_t char_count = count_utf8_chars(str, pos);
    return data::any{std::in_place_type<runtime_t<kind::int8>>, char_count};
}

static data::any round_decimal(data::any src, int32_t precision, evaluator_context& ctx,
    int32_t min_precision, int32_t max_precision) {
    if (precision < min_precision || precision > max_precision) {
        ctx.add_error({error_kind::unsupported, "scale out of range: must be between -38 and 38"});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }
    auto value  = src.to<runtime_t<kind::decimal>>();
    auto type   = std::in_place_type<runtime_t<kind::decimal>>;
    auto one    = data::any{type, takatori::decimal::triple{+1, 0, 1, -precision}};
    auto remain = expr::remainder_any(src, one);
    auto check  = remain.to<runtime_t<kind::decimal>>();
    if (check.coefficient_high() == 0 && check.coefficient_low() == 0) { return src; }
    if (value.sign() > 0) {
        auto half_value = takatori::decimal::triple{+1, 0, 5, -precision - 1};
        auto half       = data::any{type, half_value};
        auto res        = expr::subtract_any(src, remain);
        auto com =
            expr::compare_any(takatori::scalar::comparison_operator::greater_equal, remain, half);
        if (static_cast<bool>(com.to<runtime_t<kind::boolean>>())) {
            return expr::add_any(res, one);
        }
        return res;
    }
    auto minus_half_value = takatori::decimal::triple{-1, 0, 5, -precision - 1};
    auto minus_half       = data::any{type, minus_half_value};
    auto res              = expr::subtract_any(src, remain);
    auto com =
        expr::compare_any(takatori::scalar::comparison_operator::less_equal, remain, minus_half);
    if (static_cast<bool>(com.to<runtime_t<kind::boolean>>())) {
        return expr::subtract_any(res, one);
    }
    return res;
}

template <jogasaki::meta::field_type_kind Kind>
static data::any round_integral(data::any src, int32_t precision, evaluator_context& ctx,
    int32_t min_precision, const char* type_name) {
    using T = runtime_t<Kind>;

    if (precision < min_precision || precision > 0) {
        ctx.add_error({error_kind::unsupported, std::string("scale out of range for ") + type_name +
                                                    ": must be between " +
                                                    std::to_string(min_precision) + " and 0"});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }

    T divide = 1;
    for (int32_t i = 0; i < -precision; ++i) {
        divide *= 10;
    }

    auto value     = src.to<T>();
    auto fix_value = (value / divide) * divide;
    auto remain    = value - fix_value;

    if (remain > 0 && remain >= divide / 2) {
        fix_value += divide;
    } else if (remain < 0 && remain <= -divide / 2) {
        fix_value -= divide;
    }

    return data::any{std::in_place_type<T>, fix_value};
}

template <jogasaki::meta::field_type_kind Kind>
static data::any round_floating_point(data::any src, int32_t precision, evaluator_context& ctx,
    int32_t min_precision, int32_t max_precision, const char* type_name) {
    using T          = runtime_t<Kind>;
    using FactorType = std::conditional_t<std::is_same_v<T, float>, float, double>;

    if (precision < min_precision || precision > max_precision) {
        ctx.add_error({error_kind::unsupported,
            std::string("scale out of range for ") + type_name + ": must be between " +
                std::to_string(min_precision) + " and " + std::to_string(max_precision)});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }

    auto value  = src.to<T>();
    auto factor = std::pow(static_cast<FactorType>(10.0), static_cast<FactorType>(precision));
    auto result = std::round(value * factor) / factor;

    return data::any{std::in_place_type<T>, result};
}
static data::any round(data::any src, int32_t precision, evaluator_context& ctx) {
    switch (src.type_index()) {
        case data::any::index<runtime_t<kind::int4>>:
            return round_integral<kind::int4>(src, precision, ctx, -9, "INT");
        case data::any::index<runtime_t<kind::int8>>:
            return round_integral<kind::int8>(src, precision, ctx, -18, "BIGINT");
        case data::any::index<runtime_t<kind::float4>>:
            return round_floating_point<kind::float4>(src, precision, ctx, -7, 7, "REAL");
        case data::any::index<runtime_t<kind::float8>>:
            return round_floating_point<kind::float8>(src, precision, ctx, -15, 15, "DOUBLE");
        case data::any::index<runtime_t<kind::decimal>>:
            return round_decimal(src, precision, ctx, -38, 38);
        default: std::abort();
    }
}

} // namespace impl

data::any substring(evaluator_context&, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 2 || args.size() == 3); // NOLINT

    std::optional<std::reference_wrapper<data::any>> length;
    std::optional<runtime_t<kind::int8>> casted_length;

    auto& src = static_cast<data::any&>(args[0]);
    if (src.empty()) { return {}; }

    auto& start = static_cast<data::any&>(args[1]);
    if (start.empty()) { return {}; }

    const auto zero_based_start = start.to<runtime_t<kind::int8>>() - 1;

    if (args.size() > 2) {
        length = static_cast<data::any&>(args[2]);
        if (!length || length->get().empty()) { return {}; }
        casted_length = length->get().to<runtime_t<kind::int8>>();
    }
    if (src.type_index() == data::any::index<accessor::binary>) {
        auto bin = src.to<runtime_t<kind::octet>>();
        return impl::extract_substring(static_cast<std::string_view>(bin),
            std::in_place_type<runtime_t<kind::octet>>, zero_based_start, casted_length);
    }
    if (src.type_index() == data::any::index<accessor::text>) {
        auto text = src.to<runtime_t<kind::character>>();
        auto str  = static_cast<std::string_view>(text);
        if (!impl::is_valid_utf8(str)) { return {}; }
        return impl::extract_substring(
            str, std::in_place_type<runtime_t<kind::character>>, zero_based_start, casted_length);
    }

    std::abort();
}

data::any upper(evaluator_context& ctx, sequence_view<data::any> args) {
    auto& src = static_cast<data::any&>(args[0]);
    if (src.empty()) { return {}; }
    if (src.type_index() == data::any::index<accessor::text>) {
        return impl::convert_case<runtime_t<kind::character>>(
            ctx, src, [](char c) { return (c >= 'a' && c <= 'z') ? (c - 0x20) : c; });
    }
    std::abort();
}

data::any lower(evaluator_context& ctx, sequence_view<data::any> args) {
    auto& src = static_cast<data::any&>(args[0]);
    if (src.empty()) { return {}; }
    if (src.type_index() == data::any::index<accessor::text>) {
        return impl::convert_case<runtime_t<kind::character>>(
            ctx, src, [](char c) { return (c >= 'A' && c <= 'Z') ? (c + 0x20) : c; });
    }
    std::abort();
}

data::any character_length(evaluator_context&, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 1); // NOLINT
    auto& src = static_cast<data::any&>(args[0]);
    if (src.empty()) { return {}; }
    if (src.type_index() == data::any::index<accessor::text>) {
        auto text        = src.to<runtime_t<kind::character>>();
        if (auto len = impl::get_utf8_length(static_cast<std::string_view>(text))) {
            return data::any{std::in_place_type<runtime_t<kind::int8>>, *len};
        }
        return {};
    }
    std::abort();
}

data::any abs(evaluator_context& ctx, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 1); // NOLINT
    auto& src = static_cast<data::any&>(args[0]);
    if (src.empty()) { return {}; }
    switch (src.type_index()) {
        case data::any::index<runtime_t<kind::int4>>: {
            auto abs_value = src.to<runtime_t<kind::int4>>();
            if (abs_value == std::numeric_limits<runtime_t<kind::int4>>::min()) {
                ctx.add_error({error_kind::overflow,
                    "integer out of range: cannot convert INT minimum value."});
                return data::any{std::in_place_type<error>, error(error_kind::overflow)};
            }
            return data::any{std::in_place_type<runtime_t<kind::int4>>, impl::abs_val(abs_value)};
        }
        case data::any::index<runtime_t<kind::int8>>: {
            auto abs_value = src.to<runtime_t<kind::int8>>();
            if (abs_value == std::numeric_limits<runtime_t<kind::int8>>::min()) {
                ctx.add_error({error_kind::overflow,
                    "integer out of range: cannot convert BIGINT minimum value."});
                return data::any{std::in_place_type<error>, error(error_kind::overflow)};
            }
            return data::any{std::in_place_type<runtime_t<kind::int8>>, impl::abs_val(abs_value)};
        }
        case data::any::index<runtime_t<kind::float4>>: {
            auto abs_value = src.to<runtime_t<kind::float4>>();
            return data::any{std::in_place_type<runtime_t<kind::float4>>, std::fabs(abs_value)};
        }
        case data::any::index<runtime_t<kind::float8>>: {
            auto abs_value = src.to<runtime_t<kind::float8>>();
            return data::any{std::in_place_type<runtime_t<kind::float8>>, std::fabs(abs_value)};
        }
        case data::any::index<runtime_t<kind::decimal>>: {
            auto value     = src.to<runtime_t<kind::decimal>>();
            auto new_value = takatori::decimal::triple{
                +1, value.coefficient_high(), value.coefficient_low(), value.exponent()};
            return data::any{std::in_place_type<runtime_t<kind::decimal>>, new_value};
        }
        default: std::abort();
    }
    std::abort();
}

data::any position(evaluator_context&, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 2); // NOLINT

    auto& lstr = static_cast<data::any&>(args[0]);
    if (lstr.empty()) { return {}; }

    auto& rstr = static_cast<data::any&>(args[1]);
    if (rstr.empty()) { return {}; }
    if (lstr.type_index() == data::any::index<accessor::text>) {
        auto ltext  = lstr.to<runtime_t<kind::character>>();
        auto substr = static_cast<std::string_view>(ltext);
        if (substr.empty()) { return data::any{std::in_place_type<runtime_t<kind::int8>>, 1}; }
        if (rstr.type_index() == data::any::index<accessor::text>) {
            auto rtext = rstr.to<runtime_t<kind::character>>();
            auto str   = static_cast<std::string_view>(rtext);
            if (!impl::is_valid_utf8(str)) { return {}; }
            if (str.empty()) { return data::any{std::in_place_type<runtime_t<kind::int8>>, 0}; }
            return impl::extract_position(substr, str);
        }
    }
    std::abort();
}

data::any mod(evaluator_context&, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 2); // NOLINT
    auto& first = static_cast<data::any&>(args[0]);
    if (first.empty()) { return {}; }
    auto& second = static_cast<data::any&>(args[1]);
    if (second.empty()) { return {}; }
    return expr::remainder_any(first, second);
}

data::any ceil(evaluator_context&, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 1); // NOLINT
    auto& src = static_cast<data::any&>(args[0]);
    if (src.empty()) { return {}; }
    switch (src.type_index()) {
        case data::any::index<runtime_t<kind::int4>>: {
            return src;
        }
        case data::any::index<runtime_t<kind::int8>>: {
            return src;
        }
        case data::any::index<runtime_t<kind::float4>>: {
            auto value = src.to<runtime_t<kind::float4>>();
            return data::any{std::in_place_type<runtime_t<kind::float4>>, std::ceil(value)};
        }
        case data::any::index<runtime_t<kind::float8>>: {
            auto value = src.to<runtime_t<kind::float8>>();
            return data::any{std::in_place_type<runtime_t<kind::float8>>, std::ceil(value)};
        }
        case data::any::index<runtime_t<kind::decimal>>: {
            auto value = src.to<runtime_t<kind::decimal>>();
            auto type  = std::in_place_type<runtime_t<kind::decimal>>;
            auto sign  = value.sign();
            auto exp   = value.exponent();
            if (sign == 0 || exp >= 0) { return src; }
            constexpr auto one_value = takatori::decimal::triple{+1, 0, 1, 0};
            auto one                 = data::any{type, one_value};
            auto remain              = expr::remainder_any(src, one);
            auto res                 = expr::subtract_any(src, remain);
            if (sign == 1) {
                auto check = remain.to<runtime_t<kind::decimal>>();
                if (check.coefficient_high() == 0 && check.coefficient_low() == 0) { return res; }
                return expr::add_any(res, one);
            }
            return res;
        }
        default: std::abort();
    }
    std::abort();
}
data::any floor(evaluator_context&, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 1); // NOLINT
    auto& src = static_cast<data::any&>(args[0]);
    if (src.empty()) { return {}; }
    switch (src.type_index()) {
        case data::any::index<runtime_t<kind::int4>>: {
            return src;
        }
        case data::any::index<runtime_t<kind::int8>>: {
            return src;
        }
        case data::any::index<runtime_t<kind::float4>>: {
            auto value = src.to<runtime_t<kind::float4>>();
            return data::any{std::in_place_type<runtime_t<kind::float4>>, std::floor(value)};
        }
        case data::any::index<runtime_t<kind::float8>>: {
            auto value = src.to<runtime_t<kind::float8>>();
            return data::any{std::in_place_type<runtime_t<kind::float8>>, std::floor(value)};
        }
        case data::any::index<runtime_t<kind::decimal>>: {
            auto value = src.to<runtime_t<kind::decimal>>();
            auto type  = std::in_place_type<runtime_t<kind::decimal>>;
            auto sign  = value.sign();
            auto exp   = value.exponent();
            if (sign == 0 || exp >= 0) { return src; }
            constexpr auto one_value = takatori::decimal::triple{+1, 0, 1, 0};
            auto one                 = data::any{type, one_value};
            auto remain              = expr::remainder_any(src, one);
            auto res                 = expr::subtract_any(src, remain);
            if (sign == -1) {
                auto check = remain.to<runtime_t<kind::decimal>>();
                if (check.coefficient_high() == 0 && check.coefficient_low() == 0) { return res; }
                return expr::subtract_any(res, one);
            }
            return res;
        }
        default: std::abort();
    }
    std::abort();
}

data::any round(evaluator_context& ctx, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 1 || args.size() == 2); // NOLINT
    auto& src = static_cast<data::any&>(args[0]);
    if (src.empty()) { return {}; }
    runtime_t<kind::int4> scale_int4{0};
    if (args.size() > 1) {
        auto scale = static_cast<data::any&>(args[1]);
        if (scale.empty()) { return {}; }
        switch (scale.type_index()) {
            case data::any::index<runtime_t<kind::int4>>: {
                scale_int4 = scale.to<runtime_t<kind::int4>>();
                if (scale_int4 < -38 || scale_int4 > 38) {
                    ctx.add_error({error_kind::unsupported,
                        "scale out of range: must be between -38 and 38"});
                    return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
                }
                break;
            }
            case data::any::index<runtime_t<kind::int8>>: {
                auto scale_int8 = scale.to<runtime_t<kind::int8>>();
                if (scale_int8 < -38 || scale_int8 > 38) {
                    ctx.add_error({error_kind::unsupported,
                        "scale out of range: must be between -38 and 38"});
                    return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
                }
                scale_int4 = static_cast<runtime_t<kind::int4>>(scale_int8);
                break;
            }
            default: std::abort();
        }
    }
    return impl::round(src, scale_int4, ctx);
}

data::any encode(evaluator_context& ctx, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 2); // NOLINT
    auto& src_arg = static_cast<data::any&>(args[0]);
    auto& enc_arg = static_cast<data::any&>(args[1]);
    if (src_arg.empty()) { return {}; }
    if (enc_arg.empty()) {
        ctx.add_error({error_kind::unsupported, "encode must be specified"});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }
    if (enc_arg.type_index() != data::any::index<accessor::text>) {
        ctx.add_error({error_kind::unsupported, "encode must be varchar"});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }
    auto encoding_text = enc_arg.to<runtime_t<kind::character>>();
    auto encoding      = static_cast<std::string_view>(encoding_text);
    if (!boost::iequals(encoding, "base64")) {
        ctx.add_error({error_kind::unsupported, "encode must be base64"});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }

    if (src_arg.type_index() == data::any::index<accessor::binary>) {
        auto bin      = src_arg.to<runtime_t<kind::octet>>();
        auto bin_data = static_cast<std::string_view>(bin);
        auto encoded_data = utils::encode_base64(bin_data);
        if (encoded_data.size() > character_type_max_length_for_value) {
            std::string error_message = std::string("value is too long to encode length:") +
                                        std::to_string(encoded_data.size()) + " maximum:" +
                                        std::to_string(character_type_max_length_for_value);
            ctx.set_error_info(create_error_info(error_code::value_too_long_exception,
                error_message, status::err_invalid_runtime_value));
            return data::any{std::in_place_type<error>, error(error_kind::error_info_provided)};
        }
        return data::any{std::in_place_type<runtime_t<kind::character>>,
            runtime_t<kind::character>{ctx.resource(), encoded_data}};
    }
    std::abort();
}
data::any decode(evaluator_context& ctx, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 2); // NOLINT
    auto& src_arg = static_cast<data::any&>(args[0]);
    auto& enc_arg = static_cast<data::any&>(args[1]);
    if (src_arg.empty()) { return {}; }
    if (enc_arg.empty()) {
        ctx.add_error({error_kind::unsupported, "encode must be specified"});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }
    if (enc_arg.type_index() != data::any::index<accessor::text>) {
        ctx.add_error({error_kind::unsupported, "encode must be varchar"});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }
    auto encoding_text = enc_arg.to<runtime_t<kind::character>>();
    auto encoding      = static_cast<std::string_view>(encoding_text);
    if (!boost::iequals(encoding, "base64")) {
        ctx.add_error({error_kind::unsupported, "encode must be base64"});
        return data::any{std::in_place_type<error>, error(error_kind::unsupported)};
    }
    if (src_arg.type_index() == data::any::index<accessor::text>) {
        auto ch      = src_arg.to<runtime_t<kind::character>>();
        auto ch_data = static_cast<std::string_view>(ch);
        if (ch_data.empty()){
            return data::any{std::in_place_type<runtime_t<kind::octet>>,
            runtime_t<kind::octet>{ctx.resource(), ""}};
        }
        if (!utils::is_base64(ch_data)) {
            ctx.add_error({error_kind::invalid_input_value, "invalid base64 characters"});
            return data::any{std::in_place_type<error>, error(error_kind::invalid_input_value)};
        }
        auto decoded_data = utils::decode_base64(ch_data);
        if (decoded_data.size() > octet_type_max_length_for_value) {
            std::string error_message = std::string("value is too long to decode length:") +
                                        std::to_string(decoded_data.size()) + " maximum:" +
                                        std::to_string(octet_type_max_length_for_value);
            ctx.set_error_info(create_error_info(error_code::value_too_long_exception,
                error_message, status::err_invalid_runtime_value));
            return data::any{std::in_place_type<error>, error(error_kind::error_info_provided)};
        }
        return data::any{std::in_place_type<runtime_t<kind::octet>>,
            runtime_t<kind::octet>{ctx.resource(), decoded_data}};
    }
    std::abort();
}

data::any rtrim(evaluator_context& ctx, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 1); // NOLINT
    auto& src_arg = static_cast<data::any&>(args[0]);
    if (src_arg.empty()) { return {}; }
    if (src_arg.type_index() == data::any::index<accessor::text>) {
        auto ch      = src_arg.to<runtime_t<kind::character>>();
        auto ch_data = static_cast<std::string_view>(ch);
        return data::any{std::in_place_type<runtime_t<kind::character>>,
            runtime_t<kind::character>{ctx.resource(), utils::rtrim(ch_data)}};
    }
    std::abort();
}
data::any ltrim(evaluator_context& ctx, sequence_view<data::any> args) {
    BOOST_ASSERT(args.size() == 1); // NOLINT
    auto& src_arg = static_cast<data::any&>(args[0]);
    if (src_arg.empty()) { return {}; }
    if (src_arg.type_index() == data::any::index<accessor::text>) {
        auto ch      = src_arg.to<runtime_t<kind::character>>();
        auto ch_data = static_cast<std::string_view>(ch);
        return data::any{std::in_place_type<runtime_t<kind::character>>,
            runtime_t<kind::character>{ctx.resource(), utils::ltrim(ch_data)}};
    }
    std::abort();
}

} // namespace builtin

} // namespace jogasaki::executor::function
