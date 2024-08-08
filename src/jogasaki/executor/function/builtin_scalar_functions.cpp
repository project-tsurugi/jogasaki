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
#include <jogasaki/executor/function/scalar_function_info.h>
#include <jogasaki/executor/function/scalar_function_kind.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/function/field_locator.h>
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
        auto octet_length = std::make_shared<scalar_function_info>(
            scalar_function_kind::octet_length,
            builtin::octet_length
        );
        auto name = "octet_length";
        repo.add(id, octet_length);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::character(t::varying),
            },
        });
        repo.add(id, octet_length);
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
        auto current_date = std::make_shared<scalar_function_info>(
            scalar_function_kind::octet_length,
            builtin::current_date,
            0
        );
        auto name = "current_date";
        repo.add(id, current_date);
        functions.add({
            id++,
            name,
            t::date(),
            {},
        });
    }
}

namespace builtin {

data::any octet_length(
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

data::any current_date(
    sequence_view<data::any> args
) {
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    return data::any{std::in_place_type<runtime_t<kind::date>>, takatori::datetime::date{2000, 1, 1}};
}

}  // namespace builtin

}  // namespace jogasaki::executor::function
