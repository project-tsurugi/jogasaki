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
#include "coder.h"

#include <takatori/util/exception.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/logging.h>

namespace jogasaki::kvs {

using takatori::util::throw_exception;

namespace details {

status catch_domain_error(std::function<status(void)> fn) {
    try {
        return fn();
    } catch (std::domain_error& e) {
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
    writable_stream& dest
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        auto vi = spec.storage();
        switch(type.kind()) {
            case kind::boolean: return dest.write<runtime_t<kind::boolean>>(src.get_value<runtime_t<kind::boolean>>(offset), odr);
            case kind::int1: return dest.write<runtime_t<kind::int1>>(src.get_value<runtime_t<kind::int1>>(offset), odr);
            case kind::int2: return dest.write<runtime_t<kind::int2>>(src.get_value<runtime_t<kind::int2>>(offset), odr);
            case kind::int4: return dest.write<runtime_t<kind::int4>>(src.get_value<runtime_t<kind::int4>>(offset), odr);
            case kind::int8: return dest.write<runtime_t<kind::int8>>(src.get_value<runtime_t<kind::int8>>(offset), odr);
            case kind::float4: return dest.write<runtime_t<kind::float4>>(src.get_value<runtime_t<kind::float4>>(offset), odr);
            case kind::float8: return dest.write<runtime_t<kind::float8>>(src.get_value<runtime_t<kind::float8>>(offset), odr);
            case kind::character: return dest.write<runtime_t<kind::character>>(src.get_value<runtime_t<kind::character>>(offset), odr, vi.add_padding(), vi.length());
            case kind::octet: return dest.write<runtime_t<kind::octet>>(src.get_value<runtime_t<kind::octet>>(offset), odr, vi.length());
            case kind::decimal: return dest.write<runtime_t<kind::decimal>>(src.get_value<runtime_t<kind::decimal>>(offset), odr, *type.option<kind::decimal>());
            case kind::date: return dest.write<runtime_t<kind::date>>(src.get_value<runtime_t<kind::date>>(offset), odr);
            case kind::time_of_day: return dest.write<runtime_t<kind::time_of_day>>(src.get_value<runtime_t<kind::time_of_day>>(offset), odr);
            case kind::time_point: return dest.write<runtime_t<kind::time_point>>(src.get_value<runtime_t<kind::time_point>>(offset), odr);
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
    writable_stream& dest
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        bool is_null = src.is_null(nullity_offset);
        if(auto res = dest.write<runtime_t<kind::boolean>>(is_null ? 0 : 1, odr); res != status::ok) {
            return res;
        }
        if (! is_null) {
            return encode(src, offset, type, spec, dest);
        }
        return status::ok;
    });
}

status encode(
    data::any const& src,
    meta::field_type const& type,
    coding_spec spec,
    writable_stream& dest
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        if(src.empty()) throw_exception(std::domain_error{"unexpected null value"});
        auto odr = spec.ordering();
        auto vi = spec.storage();
        switch(type.kind()) {
            case kind::boolean: return dest.write<runtime_t<kind::boolean>>(src.to<runtime_t<kind::boolean>>(), odr);
            case kind::int1: return dest.write<runtime_t<kind::int1>>(src.to<runtime_t<kind::int1>>(), odr);
            case kind::int2: return dest.write<runtime_t<kind::int2>>(src.to<runtime_t<kind::int2>>(), odr);
            case kind::int4: return dest.write<runtime_t<kind::int4>>(src.to<runtime_t<kind::int4>>(), odr);
            case kind::int8: return dest.write<runtime_t<kind::int8>>(src.to<runtime_t<kind::int8>>(), odr);
            case kind::float4: return dest.write<runtime_t<kind::float4>>(src.to<runtime_t<kind::float4>>(), odr);
            case kind::float8: return dest.write<runtime_t<kind::float8>>(src.to<runtime_t<kind::float8>>(), odr);
            case kind::character: return dest.write<runtime_t<kind::character>>(src.to<runtime_t<kind::character>>(), odr, vi.add_padding(), vi.length());
            case kind::octet: return dest.write<runtime_t<kind::octet>>(src.to<runtime_t<kind::octet>>(), odr, vi.length());
            case kind::decimal: return dest.write<runtime_t<kind::decimal>>(src.to<runtime_t<kind::decimal>>(), odr, *type.option<kind::decimal>());
            case kind::date: return dest.write<runtime_t<kind::date>>(src.to<runtime_t<kind::date>>(), odr);
            case kind::time_of_day: return dest.write<runtime_t<kind::time_of_day>>(src.to<runtime_t<kind::time_of_day>>(), odr);
            case kind::time_point: return dest.write<runtime_t<kind::time_point>>(src.to<runtime_t<kind::time_point>>(), odr);
            default: break;
        }
        throw_exception(std::domain_error{"Unsupported types or metadata corruption"});
    });
}

status encode_nullable(
    data::any const& src,
    meta::field_type const& type,
    coding_spec spec,
    writable_stream& dest
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        bool is_null = src.empty();
        if(auto res = dest.write<runtime_t<kind::boolean>>(is_null ? 0 : 1, odr); res != status::ok) {
            return res;
        }
        if(! is_null) {
            return encode(src, type, spec, dest);
        }
        return status::ok;
    });
}

status decode(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    data::any& dest,
    memory::paged_memory_resource* resource
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        using any = data::any;
        auto odr = spec.ordering();
        switch(type.kind()) {
            case kind::boolean: dest = any{std::in_place_type<runtime_t<kind::boolean>>, src.read<runtime_t<kind::boolean>>(odr, false)}; break;
            case kind::int1: dest = any{std::in_place_type<runtime_t<kind::int1>>, src.read<runtime_t<kind::int1>>(odr, false)}; break;
            case kind::int2: dest = any{std::in_place_type<runtime_t<kind::int2>>, src.read<runtime_t<kind::int2>>(odr, false)}; break;
            case kind::int4: dest = any{std::in_place_type<runtime_t<kind::int4>>, src.read<runtime_t<kind::int4>>(odr, false)}; break;
            case kind::int8: dest = any{std::in_place_type<runtime_t<kind::int8>>, src.read<runtime_t<kind::int8>>(odr, false)}; break;
            case kind::float4: dest = any{std::in_place_type<runtime_t<kind::float4>>, src.read<runtime_t<kind::float4>>(odr, false)}; break;
            case kind::float8: dest = any{std::in_place_type<runtime_t<kind::float8>>, src.read<runtime_t<kind::float8>>(odr, false)}; break;
            case kind::character: dest = any{std::in_place_type<runtime_t<kind::character>>, src.read<runtime_t<kind::character>>(odr, false, resource)}; break;
            case kind::octet: dest = any{std::in_place_type<runtime_t<kind::octet>>, src.read<runtime_t<kind::octet>>(odr, false, resource)}; break;
            case kind::decimal: dest = any{std::in_place_type<runtime_t<kind::decimal>>, src.read<runtime_t<kind::decimal>>(odr, false, *type.option<kind::decimal>())}; break;
            case kind::date: dest = any{std::in_place_type<runtime_t<kind::date>>, src.read<runtime_t<kind::date>>(odr, false)}; break;
            case kind::time_of_day: dest = any{std::in_place_type<runtime_t<kind::time_of_day>>, src.read<runtime_t<kind::time_of_day>>(odr, false)}; break;
            case kind::time_point: dest = any{std::in_place_type<runtime_t<kind::time_point>>, src.read<runtime_t<kind::time_point>>(odr, false)}; break;
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
    accessor::record_ref dest,
    std::size_t offset,
    memory::paged_memory_resource* resource
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        switch(type.kind()) {
            case kind::boolean: dest.set_value<runtime_t<kind::boolean>>(offset, src.read<runtime_t<kind::boolean>>(odr, false)); break;
            case kind::int1: dest.set_value<runtime_t<kind::int1>>(offset, src.read<runtime_t<kind::int1>>(odr, false)); break;
            case kind::int2: dest.set_value<runtime_t<kind::int2>>(offset, src.read<runtime_t<kind::int2>>(odr, false)); break;
            case kind::int4: dest.set_value<runtime_t<kind::int4>>(offset, src.read<runtime_t<kind::int4>>(odr, false)); break;
            case kind::int8: dest.set_value<runtime_t<kind::int8>>(offset, src.read<runtime_t<kind::int8>>(odr, false)); break;
            case kind::float4: dest.set_value<runtime_t<kind::float4>>(offset, src.read<runtime_t<kind::float4>>(odr, false)); break;
            case kind::float8: dest.set_value<runtime_t<kind::float8>>(offset, src.read<runtime_t<kind::float8>>(odr, false)); break;
            case kind::character: dest.set_value<runtime_t<kind::character>>(offset, src.read<runtime_t<kind::character>>(odr, false, resource)); break;
            case kind::octet: dest.set_value<runtime_t<kind::octet>>(offset, src.read<runtime_t<kind::octet>>(odr, false, resource)); break;
            case kind::decimal: dest.set_value<runtime_t<kind::decimal>>(offset, src.read<runtime_t<kind::decimal>>(odr, false, *type.option<kind::decimal>())); break;
            case kind::date: dest.set_value<runtime_t<kind::date>>(offset, src.read<runtime_t<kind::date>>(odr, false)); break;
            case kind::time_of_day: dest.set_value<runtime_t<kind::time_of_day>>(offset, src.read<runtime_t<kind::time_of_day>>(odr, false)); break;
            case kind::time_point: dest.set_value<runtime_t<kind::time_point>>(offset, src.read<runtime_t<kind::time_point>>(odr, false)); break;
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
    accessor::record_ref dest,
    std::size_t offset,
    std::size_t nullity_offset,
    memory::paged_memory_resource* resource
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
        if(! (flag == 0 || flag == 1)) {
            LOG_LP(ERROR) << "unexpected data in nullity bit:" << flag;
            return status::err_data_corruption;
        }
        bool is_null = flag == 0;
        dest.set_null(nullity_offset, is_null);
        if (! is_null) {
            return decode(src, type, spec, dest, offset, resource);
        }
        return status::ok;
    });
}

status decode_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    data::any& dest,
    memory::paged_memory_resource* resource
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
        if(! (flag == 0 || flag == 1)) {
            LOG_LP(ERROR) << "unexpected data in nullity bit:" << flag;
            return status::err_data_corruption;
        }
        bool is_null = flag == 0;
        if (is_null) {
            dest = {};
            return status::ok;
        }
        return decode(src, type, spec, dest, resource);
    });
}

status consume_stream(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        switch(type.kind()) {
            case kind::boolean: src.read<runtime_t<kind::boolean>>(odr, true); break;
            case kind::int1: src.read<runtime_t<kind::int1>>(odr, true); break;
            case kind::int2: src.read<runtime_t<kind::int2>>(odr, true); break;
            case kind::int4: src.read<runtime_t<kind::int4>>(odr, true); break;
            case kind::int8: src.read<runtime_t<kind::int8>>(odr, true); break;
            case kind::float4: src.read<runtime_t<kind::float4>>(odr, true); break;
            case kind::float8: src.read<runtime_t<kind::float8>>(odr, true); break;
            case kind::character: src.read<runtime_t<kind::character>>(odr, true, nullptr); break;
            case kind::octet: src.read<runtime_t<kind::octet>>(odr, true, nullptr); break;
            case kind::decimal: src.read<runtime_t<kind::decimal>>(odr, true, *type.option<kind::decimal>()); break;
            case kind::date: src.read<runtime_t<kind::date>>(odr, true); break;
            case kind::time_of_day: src.read<runtime_t<kind::time_of_day>>(odr, true); break;
            case kind::time_point: src.read<runtime_t<kind::time_point>>(odr, true); break;
            default:
                throw_exception(std::domain_error{"Unsupported types or metadata corruption"});
        }
        return status::ok;
    });
}

status consume_stream_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec
) {
    return details::catch_domain_error([&]() {
        using kind = meta::field_type_kind;
        auto odr = spec.ordering();
        auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
        if(! (flag == 0 || flag == 1)) {
            LOG_LP(ERROR) << "unexpected data in nullity bit:" << flag;
            return status::err_data_corruption;
        }
        bool is_null = flag == 0;
        if (! is_null) {
            return consume_stream(src, type, spec);
        }
        return status::ok;
    });
}

}

