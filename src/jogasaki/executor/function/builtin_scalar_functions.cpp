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

bool is_continuation_byte(unsigned char c) { return (c & 0xC0U) == 0x80U; }
encoding_type detect_next_encoding(std::string_view view, const size_t offset) {
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
size_t get_byte(encoding_type e) {
    switch (e) {
        case encoding_type::ASCII_1BYTE: return 1;
        case encoding_type::UTF8_2BYTE: return 2;
        case encoding_type::UTF8_3BYTE: return 3;
        case encoding_type::UTF8_4BYTE: return 4;
        case encoding_type::INVALID: return 0;
    }
    return 0;
}
size_t get_start_index_byte(
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
size_t get_size_byte(std::string_view view, const size_t start_byte, const size_t letter_count,
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
bool is_valid_utf8(std::string_view view) {
    size_t offset = 0;
    while (offset < view.size()) {
        size_t char_size = get_byte(detect_next_encoding(view, offset));
        if (char_size == 0) { return false; }
        offset += char_size;
    }
    return true;
}

size_t get_utf8_length(std::string_view view) {
    size_t offset = 0;
    size_t count  = 0;
    while (offset < view.size()) {
        size_t char_size = get_byte(detect_next_encoding(view, offset));
        if (char_size == 0) { return 0; }
        offset += char_size;
        count++;
    }
    return count;
}

template <typename TypeTag>
data::any extract_substring(std::string_view view, TypeTag type_tag, int64_t zero_based_start,
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
data::any convert_case(evaluator_context& ctx, const data::any& src, Func case_converter) {
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
        const size_t len = impl::get_utf8_length(static_cast<std::string_view>(text));
        if (len == 0) { return {}; }
        return data::any{std::in_place_type<runtime_t<kind::int8>>, len};
    }
    std::abort();
}

} // namespace builtin

} // namespace jogasaki::executor::function
