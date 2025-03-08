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
#include "builtin_scalar_functions.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <variant>
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
#include <jogasaki/data/any.h>
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

data::any tx_ts_is_available(evaluator_context& ctx) {
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

}  // namespace builtin

}  // namespace jogasaki::executor::function
