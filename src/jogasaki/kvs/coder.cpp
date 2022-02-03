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

#include <takatori/util/fail.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/readable_stream.h>

namespace jogasaki::kvs {

using takatori::util::fail;

void encode(
    accessor::record_ref src,
    std::size_t offset,
    meta::field_type const& type,
    coding_spec spec,
    writable_stream& dest
) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    auto vi = spec.varlen_info();
    switch(type.kind()) {
        case kind::boolean: dest.write<runtime_t<kind::boolean>>(src.get_value<runtime_t<kind::boolean>>(offset), odr); break;
        case kind::int1: dest.write<runtime_t<kind::int1>>(src.get_value<runtime_t<kind::int1>>(offset), odr); break;
        case kind::int2: dest.write<runtime_t<kind::int2>>(src.get_value<runtime_t<kind::int2>>(offset), odr); break;
        case kind::int4: dest.write<runtime_t<kind::int4>>(src.get_value<runtime_t<kind::int4>>(offset), odr); break;
        case kind::int8: dest.write<runtime_t<kind::int8>>(src.get_value<runtime_t<kind::int8>>(offset), odr); break;
        case kind::float4: dest.write<runtime_t<kind::float4>>(src.get_value<runtime_t<kind::float4>>(offset), odr); break;
        case kind::float8: dest.write<runtime_t<kind::float8>>(src.get_value<runtime_t<kind::float8>>(offset), odr); break;
        case kind::character: dest.write<runtime_t<kind::character>>(src.get_value<runtime_t<kind::character>>(offset), odr, vi.varying(), vi.length()); break;
        default:
            fail();
    }
}

void encode_nullable(
    accessor::record_ref src,
    std::size_t offset,
    std::size_t nullity_offset,
    meta::field_type const& type,
    coding_spec spec,
    writable_stream& dest
) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    bool is_null = src.is_null(nullity_offset);
    dest.write<runtime_t<kind::boolean>>(is_null ? 0 : 1, odr);
    if (! is_null) {
        encode(src, offset, type, spec, dest);
    }
}

void encode(
    executor::process::impl::expression::any const& src,
    meta::field_type const& type,
    coding_spec spec,
    writable_stream& dest
) {
    using kind = meta::field_type_kind;
    BOOST_ASSERT(! src.empty());  //NOLINT
    auto odr = spec.ordering();
    auto vi = spec.varlen_info();
    switch(type.kind()) {
        case kind::boolean: dest.write<runtime_t<kind::boolean>>(src.to<runtime_t<kind::boolean>>(), odr); break;
        case kind::int1: dest.write<runtime_t<kind::int1>>(src.to<runtime_t<kind::int1>>(), odr); break;
        case kind::int2: dest.write<runtime_t<kind::int2>>(src.to<runtime_t<kind::int2>>(), odr); break;
        case kind::int4: dest.write<runtime_t<kind::int4>>(src.to<runtime_t<kind::int4>>(), odr); break;
        case kind::int8: dest.write<runtime_t<kind::int8>>(src.to<runtime_t<kind::int8>>(), odr); break;
        case kind::float4: dest.write<runtime_t<kind::float4>>(src.to<runtime_t<kind::float4>>(), odr); break;
        case kind::float8: dest.write<runtime_t<kind::float8>>(src.to<runtime_t<kind::float8>>(), odr); break;
        case kind::character: dest.write<runtime_t<kind::character>>(src.to<runtime_t<kind::character>>(), odr, vi.varying(), vi.length()); break;
        default:
            fail();
    }
}

void encode_nullable(
    executor::process::impl::expression::any const& src,
    meta::field_type const& type,
    coding_spec spec,
    writable_stream& dest
) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    bool is_null = src.empty();
    dest.write<runtime_t<kind::boolean>>(is_null ? 0 : 1, odr);
    if(! is_null) {
        encode(src, type, spec, dest);
    }
}

void decode(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    executor::process::impl::expression::any& dest,
    memory::paged_memory_resource* resource
) {
    using kind = meta::field_type_kind;
    using any = executor::process::impl::expression::any;
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
        default:
            fail();
    }
}

void decode(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    accessor::record_ref dest,
    std::size_t offset,
    memory::paged_memory_resource* resource
) {
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
        default:
            fail();
    }
}

void decode_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    accessor::record_ref dest,
    std::size_t offset,
    std::size_t nullity_offset,
    memory::paged_memory_resource* resource
) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
    BOOST_ASSERT(flag == 0 || flag == 1);  //NOLINT
    bool is_null = flag == 0;
    dest.set_null(nullity_offset, is_null);
    if (! is_null) {
        decode(src, type, spec, dest, offset, resource);
    }
}

void decode_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    executor::process::impl::expression::any& dest,
    memory::paged_memory_resource* resource
) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
    BOOST_ASSERT(flag == 0 || flag == 1);  //NOLINT
    bool is_null = flag == 0;
    if (is_null) {
        dest = {};
        return;
    }
    decode(src, type, spec, dest, resource);
}

void consume_stream(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec
) {
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
        default:
            fail();
    }
}

void consume_stream_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec
) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    auto flag = src.read<runtime_t<kind::boolean>>(odr, false);
    BOOST_ASSERT(flag == 0 || flag == 1);  //NOLINT
    bool is_null = flag == 0;
    if (! is_null) {
        consume_stream(src, type, spec);
    }
}

}

