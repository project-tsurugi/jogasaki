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
#include <jogasaki/data/any.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/scalar_function_info.h>
#include <jogasaki/executor/function/scalar_function_kind.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/function/value_generator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/round.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using executor::process::impl::expression::evaluator_context;
using jogasaki::executor::process::impl::expression::error_kind;
using jogasaki::executor::process::impl::expression::error;

using kind = meta::field_type_kind;

void add_builtin_scalar_functions(
    ::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo
) {
    namespace t = takatori::type;
    using namespace ::yugawara;
    constexpr static std::size_t minimum_scalar_function_id = 1000;
    std::size_t id = ::yugawara::function::declaration::minimum_builtin_function_id
        + minimum_scalar_function_id;

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
        repo.add(id, info);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::character(t::varying),
            },
        });
        repo.add(id, info);
        functions.add({
            id++,
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
        repo.add(id, info);
        functions.add({
            id++,
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
        repo.add(id, info);
        functions.add({
            id++,
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
        repo.add(id, info);
        functions.add({
            id++,
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
        repo.add(id, info);
        functions.add({
            id++,
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
    BOOST_ASSERT(
        (src.type_index() == data::any::index<accessor::text> || src.type_index() == data::any::index<accessor::binary>
    ));  //NOLINT
    if(src.empty()) {
        return {};
    }
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
    auto fctx = ctx.func_ctx();
    if(! fctx) {
        // programming error
        ctx.add_error({error_kind::unknown, "missing function context"});
        return data::any{std::in_place_type<error>, error(error_kind::unknown)};
    }
    if(! fctx->transaction_begin().has_value()) {
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
    if(auto a = tx_ts_is_available(ctx); a.error()) {
        return a;
    }
    takatori::datetime::time_point tp{ctx.func_ctx()->transaction_begin().value()};
    // Contrary to the name `current_date`, it returns the date part of the local timestamp.
    // But system clock returns chrono::time_point in UTC, so we need to convert it to local timestamp.
    // FIXME: get the system offset and convert to local
    return data::any{std::in_place_type<runtime_t<kind::date>>, tp.date()};
}

data::any localtime(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    if(auto a = tx_ts_is_available(ctx); a.error()) {
        return a;
    }
    // This func. returns the time part of the local timestamp.
    // But system clock returns chrono::time_point in UTC, so we need to convert it to local timestamp.
    // FIXME: get the system offset and convert to local
    takatori::datetime::time_point tp{ctx.func_ctx()->transaction_begin().value()};
    return data::any{std::in_place_type<runtime_t<kind::time_of_day>>, tp.time()};
}

data::any current_timestamp(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    // same as localtimestamp
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    if(auto a = tx_ts_is_available(ctx); a.error()) {
        return a;
    }
    takatori::datetime::time_point tp{ctx.func_ctx()->transaction_begin().value()};
    return data::any{std::in_place_type<runtime_t<kind::time_point>>, tp};
}

data::any localtimestamp(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    // same as current_timestamp
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    if(auto a = tx_ts_is_available(ctx); a.error()) {
        return a;
    }
    // Unlike localtime or current_date, this function doesn't need to convert the system clock to local.
    // Because the type "WITH TIME ZONE" values are internally UTC and converted when they are passed back to clients.
    takatori::datetime::time_point tp{ctx.func_ctx()->transaction_begin().value()};
    return data::any{std::in_place_type<runtime_t<kind::time_point>>, tp};
}

}  // namespace builtin

}  // namespace jogasaki::executor::function
