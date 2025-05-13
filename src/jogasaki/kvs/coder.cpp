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
#include "coder.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <glog/logging.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/exception.h>
#include <takatori/util/stacktrace.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/status.h>

namespace jogasaki::kvs {

using takatori::util::throw_exception;

namespace details {

/**
 * @brief wrapper function to catch encoding/decoding error
 * @param fn function to be wrapped
 * @return status returned by `fn`
 * @return status::err_data_corruption if `fn` throws `std::domain_error`
 */
status catch_domain_error(std::function<status(void)> const& fn) {
    try {
        return fn();
    } catch (std::domain_error const& e) {
        LOG_LP(ERROR) << "Unexpected data error: " << e.what();
        if(auto* tr = takatori::util::find_trace(e); tr != nullptr) {
            LOG_LP(ERROR) << *tr;
        }
        return status::err_data_corruption;
    }
}

}  // namespace details

status encode(
    accessor::record_ref src,
    std::size_t offset,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    writable_stream& dest
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        dest.set_context(ctx);
        switch(type.kind()) {
            case kind::boolean: return dest.write<runtime_t<kind::boolean>>(src.get_value<runtime_t<kind::boolean>>(offset), odr);
            case kind::int1: return dest.write<runtime_t<kind::int1>>(src.get_value<runtime_t<kind::int1>>(offset), odr);
            case kind::int2: return dest.write<runtime_t<kind::int2>>(src.get_value<runtime_t<kind::int2>>(offset), odr);
            case kind::int4: return dest.write<runtime_t<kind::int4>>(src.get_value<runtime_t<kind::int4>>(offset), odr);
            case kind::int8: return dest.write<runtime_t<kind::int8>>(src.get_value<runtime_t<kind::int8>>(offset), odr);
            case kind::float4: return dest.write<runtime_t<kind::float4>>(src.get_value<runtime_t<kind::float4>>(offset), odr);
            case kind::float8: return dest.write<runtime_t<kind::float8>>(src.get_value<runtime_t<kind::float8>>(offset), odr);
            case kind::decimal: return dest.write<runtime_t<kind::decimal>>(src.get_value<runtime_t<kind::decimal>>(offset), odr, *type.option<kind::decimal>());
            case kind::character: return dest.write<runtime_t<kind::character>>(src.get_value<runtime_t<kind::character>>(offset), odr, *type.option<kind::character>(), spec.is_key());
            case kind::octet: return dest.write<runtime_t<kind::octet>>(src.get_value<runtime_t<kind::octet>>(offset), odr, *type.option<kind::octet>(), spec.is_key());
            case kind::date: return dest.write<runtime_t<kind::date>>(src.get_value<runtime_t<kind::date>>(offset), odr);
            case kind::time_of_day: return dest.write<runtime_t<kind::time_of_day>>(src.get_value<runtime_t<kind::time_of_day>>(offset), odr);
            case kind::time_point: return dest.write<runtime_t<kind::time_point>>(src.get_value<runtime_t<kind::time_point>>(offset), odr);
            case kind::blob: return dest.write<runtime_t<kind::blob>>(src.get_value<runtime_t<kind::blob>>(offset), odr);
            case kind::clob: return dest.write<runtime_t<kind::clob>>(src.get_value<runtime_t<kind::clob>>(offset), odr);
            default: break;
        }
        throw_exception(std::domain_error{"Unsupported types or metadata corruption"});
    });
}

status encode_nullable(
    accessor::record_ref src,
    std::size_t offset,
    std::size_t nullity_offset,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    writable_stream& dest
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        dest.set_context(ctx);
        bool is_null = src.is_null(nullity_offset);
        if(auto res = dest.write<runtime_t<kind::boolean>>(is_null ? 0 : 1, odr); res != status::ok) {
            return res;
        }
        if (! is_null) {
            return encode(src, offset, type, spec, ctx, dest);
        }
        return status::ok;
    });
}

status encode(
    data::any const& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    writable_stream& dest
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        if(src.empty()) throw_exception(std::domain_error{"unexpected null value"});
        auto odr = spec.ordering();
        dest.set_context(ctx);
        switch(type.kind()) {
            case kind::boolean: return dest.write<runtime_t<kind::boolean>>(src.to<runtime_t<kind::boolean>>(), odr);
            case kind::int1: return dest.write<runtime_t<kind::int1>>(src.to<runtime_t<kind::int1>>(), odr);
            case kind::int2: return dest.write<runtime_t<kind::int2>>(src.to<runtime_t<kind::int2>>(), odr);
            case kind::int4: return dest.write<runtime_t<kind::int4>>(src.to<runtime_t<kind::int4>>(), odr);
            case kind::int8: return dest.write<runtime_t<kind::int8>>(src.to<runtime_t<kind::int8>>(), odr);
            case kind::float4: return dest.write<runtime_t<kind::float4>>(src.to<runtime_t<kind::float4>>(), odr);
            case kind::float8: return dest.write<runtime_t<kind::float8>>(src.to<runtime_t<kind::float8>>(), odr);
            case kind::decimal: return dest.write<runtime_t<kind::decimal>>(src.to<runtime_t<kind::decimal>>(), odr, *type.option<kind::decimal>());
            case kind::character: return dest.write<runtime_t<kind::character>>(src.to<runtime_t<kind::character>>(), odr, *type.option<kind::character>(), spec.is_key());
            case kind::octet: return dest.write<runtime_t<kind::octet>>(src.to<runtime_t<kind::octet>>(), odr, *type.option<kind::octet>(), spec.is_key());
            case kind::date: return dest.write<runtime_t<kind::date>>(src.to<runtime_t<kind::date>>(), odr);
            case kind::time_of_day: return dest.write<runtime_t<kind::time_of_day>>(src.to<runtime_t<kind::time_of_day>>(), odr);
            case kind::time_point: return dest.write<runtime_t<kind::time_point>>(src.to<runtime_t<kind::time_point>>(), odr);
            case kind::blob: return dest.write<runtime_t<kind::blob>>(src.to<runtime_t<kind::blob>>(), odr);
            case kind::clob: return dest.write<runtime_t<kind::clob>>(src.to<runtime_t<kind::clob>>(), odr);
            default: break;
        }
        throw_exception(std::domain_error{"Unsupported types or metadata corruption"});
    });
}

status encode_nullable(
    data::any const& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    writable_stream& dest
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        dest.set_context(ctx);
        bool is_null = src.empty();
        if(auto res = dest.write<runtime_t<kind::boolean>>(is_null ? 0 : 1, odr); res != status::ok) {
            return res;
        }
        if(! is_null) {
            return encode(src, type, spec, ctx, dest);
        }
        return status::ok;
    });
}

status decode(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    data::any& dest,
    memory::paged_memory_resource* resource
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        using any = data::any;
        auto odr = spec.ordering();
        src.set_context(ctx);
        switch(type.kind()) {
            case kind::boolean: dest = any{std::in_place_type<runtime_t<kind::boolean>>, src.read<runtime_t<kind::boolean>>(odr, false)}; break;
            case kind::int1: dest = any{std::in_place_type<runtime_t<kind::int1>>, src.read<runtime_t<kind::int1>>(odr, false)}; break;
            case kind::int2: dest = any{std::in_place_type<runtime_t<kind::int2>>, src.read<runtime_t<kind::int2>>(odr, false)}; break;
            case kind::int4: dest = any{std::in_place_type<runtime_t<kind::int4>>, src.read<runtime_t<kind::int4>>(odr, false)}; break;
            case kind::int8: dest = any{std::in_place_type<runtime_t<kind::int8>>, src.read<runtime_t<kind::int8>>(odr, false)}; break;
            case kind::float4: dest = any{std::in_place_type<runtime_t<kind::float4>>, src.read<runtime_t<kind::float4>>(odr, false)}; break;
            case kind::float8: dest = any{std::in_place_type<runtime_t<kind::float8>>, src.read<runtime_t<kind::float8>>(odr, false)}; break;
            case kind::decimal: dest = any{std::in_place_type<runtime_t<kind::decimal>>, src.read<runtime_t<kind::decimal>>(odr, false, *type.option<kind::decimal>())}; break;
            case kind::character: dest = any{std::in_place_type<runtime_t<kind::character>>, src.read<runtime_t<kind::character>>(odr, false, resource)}; break;
            case kind::octet: dest = any{std::in_place_type<runtime_t<kind::octet>>, src.read<runtime_t<kind::octet>>(odr, false, *type.option<kind::octet>(), resource)}; break;
            case kind::date: dest = any{std::in_place_type<runtime_t<kind::date>>, src.read<runtime_t<kind::date>>(odr, false)}; break;
            case kind::time_of_day: dest = any{std::in_place_type<runtime_t<kind::time_of_day>>, src.read<runtime_t<kind::time_of_day>>(odr, false)}; break;
            case kind::time_point: dest = any{std::in_place_type<runtime_t<kind::time_point>>, src.read<runtime_t<kind::time_point>>(odr, false)}; break;
            case kind::blob: dest = any{std::in_place_type<runtime_t<kind::blob>>, src.read<runtime_t<kind::blob>>(odr, false)}; break;
            case kind::clob: dest = any{std::in_place_type<runtime_t<kind::clob>>, src.read<runtime_t<kind::clob>>(odr, false)}; break;
            default:
                throw_exception(std::domain_error{"Unsupported types or metadata corruption"});
        }
        return status::ok;
    });
}

status decode(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    accessor::record_ref dest,
    std::size_t offset,
    memory::paged_memory_resource* resource
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        src.set_context(ctx);
        switch(type.kind()) {
            case kind::boolean: dest.set_value<runtime_t<kind::boolean>>(offset, src.read<runtime_t<kind::boolean>>(odr, false)); break;
            case kind::int1: dest.set_value<runtime_t<kind::int1>>(offset, src.read<runtime_t<kind::int1>>(odr, false)); break;
            case kind::int2: dest.set_value<runtime_t<kind::int2>>(offset, src.read<runtime_t<kind::int2>>(odr, false)); break;
            case kind::int4: dest.set_value<runtime_t<kind::int4>>(offset, src.read<runtime_t<kind::int4>>(odr, false)); break;
            case kind::int8: dest.set_value<runtime_t<kind::int8>>(offset, src.read<runtime_t<kind::int8>>(odr, false)); break;
            case kind::float4: dest.set_value<runtime_t<kind::float4>>(offset, src.read<runtime_t<kind::float4>>(odr, false)); break;
            case kind::float8: dest.set_value<runtime_t<kind::float8>>(offset, src.read<runtime_t<kind::float8>>(odr, false)); break;
            case kind::decimal: dest.set_value<runtime_t<kind::decimal>>(offset, src.read<runtime_t<kind::decimal>>(odr, false, *type.option<kind::decimal>())); break;
            case kind::character: dest.set_value<runtime_t<kind::character>>(offset, src.read<runtime_t<kind::character>>(odr, false, resource)); break;
            case kind::octet: dest.set_value<runtime_t<kind::octet>>(offset, src.read<runtime_t<kind::octet>>(odr, false, *type.option<kind::octet>(), resource)); break;
            case kind::date: dest.set_value<runtime_t<kind::date>>(offset, src.read<runtime_t<kind::date>>(odr, false)); break;
            case kind::time_of_day: dest.set_value<runtime_t<kind::time_of_day>>(offset, src.read<runtime_t<kind::time_of_day>>(odr, false)); break;
            case kind::time_point: dest.set_value<runtime_t<kind::time_point>>(offset, src.read<runtime_t<kind::time_point>>(odr, false)); break;
            case kind::blob: dest.set_value<runtime_t<kind::blob>>(offset, src.read<runtime_t<kind::blob>>(odr, false)); break;
            case kind::clob: dest.set_value<runtime_t<kind::clob>>(offset, src.read<runtime_t<kind::clob>>(odr, false)); break;
            default:
                throw_exception(std::domain_error{"Unsupported types or metadata corruption"});
        }
        return status::ok;
    });
}

status decode_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    accessor::record_ref dest,
    std::size_t offset,
    std::size_t nullity_offset,
    memory::paged_memory_resource* resource
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        src.set_context(ctx);
        auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
        if(! (flag == 0 || flag == 1)) {
            LOG_LP(ERROR) << "unexpected data in nullity bit:" << flag; //NOLINT
            return status::err_data_corruption;
        }
        bool is_null = flag == 0;
        dest.set_null(nullity_offset, is_null);
        if (! is_null) {
            return decode(src, type, spec, ctx, dest, offset, resource);
        }
        return status::ok;
    });
}

status decode_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    data::any& dest,
    memory::paged_memory_resource* resource
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        src.set_context(ctx);
        auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
        if(! (flag == 0 || flag == 1)) {
            LOG_LP(ERROR) << "unexpected data in nullity bit:" << flag; //NOLINT
            return status::err_data_corruption;
        }
        bool is_null = flag == 0;
        if (is_null) {
            dest = {};
            return status::ok;
        }
        return decode(src, type, spec, ctx, dest, resource);
    });
}

status consume_stream(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        src.set_context(ctx);
        switch(type.kind()) {
            case kind::boolean: src.read<runtime_t<kind::boolean>>(odr, true); break;
            case kind::int1: src.read<runtime_t<kind::int1>>(odr, true); break;
            case kind::int2: src.read<runtime_t<kind::int2>>(odr, true); break;
            case kind::int4: src.read<runtime_t<kind::int4>>(odr, true); break;
            case kind::int8: src.read<runtime_t<kind::int8>>(odr, true); break;
            case kind::float4: src.read<runtime_t<kind::float4>>(odr, true); break;
            case kind::float8: src.read<runtime_t<kind::float8>>(odr, true); break;
            case kind::decimal: src.read<runtime_t<kind::decimal>>(odr, true, *type.option<kind::decimal>()); break;
            case kind::character: src.read<runtime_t<kind::character>>(odr, true, nullptr); break;
            case kind::octet: src.read<runtime_t<kind::octet>>(odr, true, *type.option<kind::octet>(), nullptr); break;
            case kind::date: src.read<runtime_t<kind::date>>(odr, true); break;
            case kind::time_of_day: src.read<runtime_t<kind::time_of_day>>(odr, true); break;
            case kind::time_point: src.read<runtime_t<kind::time_point>>(odr, true); break;
            case kind::blob: src.read<runtime_t<kind::blob>>(odr, true); break;
            case kind::clob: src.read<runtime_t<kind::clob>>(odr, true); break;
            default:
                throw_exception(std::domain_error{"Unsupported types or metadata corruption"});
        }
        return status::ok;
    });
}

status consume_stream_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        const auto odr = spec.ordering();
        src.set_context(ctx);
        const auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
        if(! (flag == 0 || flag == 1)) {
            LOG_LP(ERROR) << "unexpected data in nullity bit:" << flag; //NOLINT
            return status::err_data_corruption;
        }
        bool is_null = flag == 0;
        if (! is_null) {
            return consume_stream(src, type, spec, ctx);
        }
        return status::ok;
    });
}

}

