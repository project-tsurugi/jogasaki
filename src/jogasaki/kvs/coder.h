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
#pragma once

#include <memory>
#include <cmath>
#include <boost/endian/conversion.hpp>
#include <glog/logging.h>

#include <takatori/util/fail.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/constants.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/process/impl/expression/any.h>

namespace jogasaki::kvs {

using takatori::util::fail;

enum class order {
    undefined,
    ascending,
    descending,
};

inline constexpr order operator~(order o) noexcept {
    switch(o) {
        case order::undefined: return order::undefined;
        case order::ascending: return order::descending;
        case order::descending: return order::ascending;
    }
}

/**
 * @brief specification on encoding/decoding
 */
class coding_spec {
public:
    /**
     * @brief create default coding spec
     */
    coding_spec() = default;

    /**
     * @brief create new coding spec
     */
    constexpr coding_spec(bool is_key, order order) : is_key_(is_key), order_(order) {}

    /**
     * @brief returns whether the key encoding rule should apply
     */
    [[nodiscard]] bool is_key() const noexcept {
        return is_key_;
    }

    /**
     * @brief returns the order
     */
    [[nodiscard]] order ordering() const noexcept {
        return order_;
    }
private:
    bool is_key_{false};
    order order_{order::undefined};
};

// predefined coding specs
constexpr coding_spec spec_key_ascending = coding_spec(true, order::ascending);
constexpr coding_spec spec_key_descending = coding_spec(true, order::descending);
constexpr coding_spec spec_value = coding_spec(false, order::undefined);

namespace details {

using text_encoding_prefix_type = std::int16_t;
constexpr static std::size_t text_encoding_prefix_type_bits = sizeof(std::int16_t) * bits_per_byte;

template<typename To, typename From>
static inline To type_change(From from) {
    static_assert(sizeof(To) == sizeof(From));
    To t{};
    std::memcpy(&t, &from, sizeof(From)); // We can expect compiler to optimize to omit this copy.
    return t;
}

template<std::size_t N> struct int_n {};
template<> struct int_n<8> { using type = std::int8_t; };
template<> struct int_n<16> { using type = std::int16_t; };
template<> struct int_n<32> { using type = std::int32_t; };
template<> struct int_n<64> { using type = std::int64_t; };

template<std::size_t N> struct float_n {};
template<> struct float_n<32> { using type = float; };
template<> struct float_n<64> { using type = double; };

template<std::size_t N> using int_t = typename int_n<N>::type;
template<std::size_t N> using uint_t = std::make_unsigned_t<int_t<N>>;
template<std::size_t N> using float_t = typename float_n<N>::type;

template<std::size_t N>
static constexpr uint_t<N> SIGN_BIT = static_cast<uint_t<N>>(1) << (N - 1); // NOLINT

template<std::size_t N>
static inline uint_t<N> key_encode(int_t<N> data, order odr) {
    auto u = type_change<uint_t<N>>(data);
    u ^= SIGN_BIT<N>;
    if (odr != order::ascending) {
        u = ~u;
    }
    return boost::endian::native_to_big(u);
}

template<std::size_t N>
static inline void key_decode(int_t<N>& value, uint_t<N> data, order odr) {
    auto u = boost::endian::big_to_native(data);
    if (odr != order::ascending) {
        u = ~u;
    }
    u ^= SIGN_BIT<N>;
    value = type_change<int_t<N>>(u);
}

// encode and decode logic for double with considering the key is ascending or not
template<std::size_t N>
static inline uint_t<N> key_encode(float_t<N> data, order odr) {
    auto u = type_change<uint_t<N>>(std::isnan(data) ? std::numeric_limits<float_t<N>>::quiet_NaN() : data);
    if ((u & SIGN_BIT<N>) == 0) {
        u ^= SIGN_BIT<N>;
    } else {
        u = ~u;
    }
    if (odr != order::ascending) {
        u = ~u;
    }
    return boost::endian::native_to_big(u);
}

template<std::size_t N>
static inline void key_decode(float_t<N>& value, uint_t<N> data, order odr) {
    auto u = boost::endian::big_to_native(data);
    if (odr != order::ascending) {
        u = ~u;
    }
    if ((u & SIGN_BIT<N>) != 0) {
        u ^= SIGN_BIT<N>;
    } else {
        u = ~u;
    }
    value = type_change<float_t<N>>(u);
}

}  // namespace details

class stream {
public:
    /**
     * @brief create object with zero capacity. This object doesn't hold data, but can be used to calculate length.
     */
    stream() = default;

    /**
     * @brief create new object
     * @param buffer pointer to buffer that this instance can use
     * @param capacity length of the buffer
     */
    stream(char* buffer, std::size_t capacity) : base_(buffer), capacity_(capacity) {}

    /**
     * @brief construct stream using string as its buffer
     * @param s string to use stream buffer
     */
    explicit stream(std::string& s) : stream(s.data(), s.capacity()) {}

    template<std::size_t N>
    void do_write(details::uint_t<N> data) {
        auto sz = N/bits_per_byte;
        BOOST_ASSERT(capacity_ == 0 || pos_ + sz <= capacity_);  //NOLINT
        if (capacity_ > 0) {
            std::memcpy(base_+pos_, reinterpret_cast<char*>(&data), sz); //NOLINT
        }
        pos_ += sz;
    }

    template<class T, std::size_t N = sizeof(T) * bits_per_byte>
    std::enable_if_t<(std::is_integral_v<T> && (N == 8 || N == 16 || N == 32 || N == 64)) ||
        (std::is_floating_point_v<T> && (N == 32 || N == 64)), void> write(T data, order odr) {
        do_write<N>(details::key_encode<N>(data, odr));
    }

    void do_write(char const* dt, std::size_t sz, order odr) {
        BOOST_ASSERT(capacity_ == 0 || pos_ + sz <= capacity_);  // NOLINT
        if (sz > 0 && capacity_ > 0) {
            if (odr == order::ascending) {
                std::memcpy(base_ + pos_, dt, sz);  // NOLINT
            } else {
                for (std::size_t i = 0; i < sz; ++i) {
                    *(base_ + pos_ + i) = ~(*(dt + i));  // NOLINT
                }
            }
        }
        pos_ += sz;
    }

    template<class T>
    std::enable_if_t<std::is_same_v<T, accessor::text>, void> write(T data, order odr) {
        std::string_view sv{data};
        // for key encoding, we are assuming the text is not so long
        BOOST_ASSERT(sv.length() < 32768); //NOLINT
        details::text_encoding_prefix_type len{static_cast<details::text_encoding_prefix_type>(sv.length())};
        do_write<details::text_encoding_prefix_type_bits>(details::key_encode<details::text_encoding_prefix_type_bits>(len, odr));
        do_write(sv.data(), sv.size(), odr);
    }

    template<std::size_t N>
    details::uint_t<N> do_read(bool discard) {
        auto sz = N/bits_per_byte;
        BOOST_ASSERT(pos_ + sz <= capacity_);  // NOLINT
        auto pos = pos_;
        pos_ += sz;
        details::uint_t<N> ret{};
        if (! discard) {
            std::memcpy(&ret, base_+pos, sz); //NOLINT
        }
        return ret;
    }

    template<class T, std::size_t N = sizeof(T) * bits_per_byte>
    std::enable_if_t<(std::is_integral_v<T> && (N == 8 || N == 16 || N == 32 || N == 64)) ||
        (std::is_floating_point_v<T> && (N == 32 || N == 64)), T> read(order odr, bool discard) {
        T value{};
        auto d = do_read<N>(discard);
        if (! discard) {
            details::key_decode<N>(value, d, odr);
        }
        return value;
    }

    template<class T>
    std::enable_if_t<std::is_same_v<T, accessor::text>, T> read(order odr, bool discard, memory::paged_memory_resource* resource = nullptr) {
        auto l = read<details::text_encoding_prefix_type>(odr, false);
        BOOST_ASSERT(l >= 0); //NOLINT
        auto len = static_cast<std::size_t>(l);
        BOOST_ASSERT(pos_ + len <= capacity_);  // NOLINT
        auto pos = pos_;
        pos_ += len;
        if (!discard && len > 0) {
            auto p = static_cast<char*>(resource->allocate(len));
            if (odr == order::ascending) {
                std::memcpy(p, base_ + pos, len);  // NOLINT
            } else {
                for (std::size_t i = 0; i < len; ++i) {
                    *(p + i) = ~(*(base_ + pos + i));  // NOLINT
                }
            }
            return accessor::text{p, static_cast<std::size_t>(len)};
        }
        return accessor::text{};
    }

    void reset() {
        pos_ = 0;
    }

    [[nodiscard]] std::size_t length() const noexcept {
        return pos_;
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return capacity_;
    }
private:
    char* base_{};
    std::size_t pos_{};
    std::size_t capacity_{};
};

/**
 * @brief encode a non-nullable field data to kvs binary representation
 * @param src the record containing data to encode
 * @param offset byte offset of the field containing data to encode
 * @param type the type of the field
 * @param spec the coding spec for the encoded field
 * @param dest the stream where the encoded data is written
 */
inline void encode(accessor::record_ref src, std::size_t offset, meta::field_type const& type, coding_spec spec, stream& dest) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    switch(type.kind()) {
        case kind::boolean: dest.write<meta::field_type_traits<kind::boolean>::runtime_type>(src.get_value<meta::field_type_traits<kind::boolean>::runtime_type>(offset), odr); break;
        case kind::int1: dest.write<meta::field_type_traits<kind::int1>::runtime_type>(src.get_value<meta::field_type_traits<kind::int1>::runtime_type>(offset), odr); break;
        case kind::int2: dest.write<meta::field_type_traits<kind::int2>::runtime_type>(src.get_value<meta::field_type_traits<kind::int2>::runtime_type>(offset), odr); break;
        case kind::int4: dest.write<meta::field_type_traits<kind::int4>::runtime_type>(src.get_value<meta::field_type_traits<kind::int4>::runtime_type>(offset), odr); break;
        case kind::int8: dest.write<meta::field_type_traits<kind::int8>::runtime_type>(src.get_value<meta::field_type_traits<kind::int8>::runtime_type>(offset), odr); break;
        case kind::float4: dest.write<meta::field_type_traits<kind::float4>::runtime_type>(src.get_value<meta::field_type_traits<kind::float4>::runtime_type>(offset), odr); break;
        case kind::float8: dest.write<meta::field_type_traits<kind::float8>::runtime_type>(src.get_value<meta::field_type_traits<kind::float8>::runtime_type>(offset), odr); break;
        case kind::character: dest.write<meta::field_type_traits<kind::character>::runtime_type>(src.get_value<meta::field_type_traits<kind::character>::runtime_type>(offset), odr); break;
        default:
            fail();
    }
}

/**
 * @brief encode a nullable field data to kvs binary representation
 * @param src the record containing data to encode
 * @param offset byte offset of the field containing data to encode
 * @param nullity_offset bit offset of the field nullity
 * @param type the type of the field
 * @param spec the coding spec for the encoded field
 * @param dest the stream where the encoded data is written
 */
inline void encode_nullable(accessor::record_ref src, std::size_t offset, std::size_t nullity_offset, meta::field_type const& type, coding_spec spec, stream& dest) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    bool is_null = src.is_null(nullity_offset);
    dest.write<meta::field_type_traits<kind::boolean>::runtime_type>(is_null ? 0 : 1, odr);
    if (! is_null) {
        encode(src, offset, type, spec, dest);
    }
}

/**
 * @brief encode a non-nullable field data to kvs binary representation
 * @param src the source data to encode
 * @param type the type of the field
 * @param spec the coding spec for the encoded field
 * @param dest the stream where the encoded data is written
 */
inline void encode(executor::process::impl::expression::any const& src, meta::field_type const& type, coding_spec spec, stream& dest) {
    using kind = meta::field_type_kind;
    BOOST_ASSERT(src.has_value());  //NOLINT
    auto odr = spec.ordering();
    switch(type.kind()) {
        case kind::boolean: dest.write<meta::field_type_traits<kind::boolean>::runtime_type>(src.to<meta::field_type_traits<kind::boolean>::runtime_type>(), odr); break;
        case kind::int1: dest.write<meta::field_type_traits<kind::int1>::runtime_type>(src.to<meta::field_type_traits<kind::int1>::runtime_type>(), odr); break;
        case kind::int2: dest.write<meta::field_type_traits<kind::int2>::runtime_type>(src.to<meta::field_type_traits<kind::int2>::runtime_type>(), odr); break;
        case kind::int4: dest.write<meta::field_type_traits<kind::int4>::runtime_type>(src.to<meta::field_type_traits<kind::int4>::runtime_type>(), odr); break;
        case kind::int8: dest.write<meta::field_type_traits<kind::int8>::runtime_type>(src.to<meta::field_type_traits<kind::int8>::runtime_type>(), odr); break;
        case kind::float4: dest.write<meta::field_type_traits<kind::float4>::runtime_type>(src.to<meta::field_type_traits<kind::float4>::runtime_type>(), odr); break;
        case kind::float8: dest.write<meta::field_type_traits<kind::float8>::runtime_type>(src.to<meta::field_type_traits<kind::float8>::runtime_type>(), odr); break;
        case kind::character: dest.write<meta::field_type_traits<kind::character>::runtime_type>(src.to<meta::field_type_traits<kind::character>::runtime_type>(), odr); break;
        default:
            fail();
    }
}

/**
 * @brief encode a nullable field data to kvs binary representation
 * @param src the source data to encode
 * @param type the type of the field
 * @param spec the coding spec for the encoded field
 * @param dest the stream where the encoded data is written
 */
inline void encode_nullable(executor::process::impl::expression::any const& src, meta::field_type const& type, coding_spec spec, stream& dest) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    bool is_null = !src.has_value();
    dest.write<meta::field_type_traits<kind::boolean>::runtime_type>(is_null ? 0 : 1, odr);
    if(! is_null) {
        encode(src, type, spec, dest);
    }
}

/**
 * @brief decode a non-nullable field's kvs binary representation to a field data
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 * @param dest the record to containing the field
 * @param offset byte offset of the field
 * @param nullity_offset bit offset of the field nullity
 * @param resource the memory resource used to generate text data. nullptr can be passed if no text field is processed.
 */
inline void decode(stream& src, meta::field_type const& type, coding_spec spec, accessor::record_ref dest, std::size_t offset, memory::paged_memory_resource* resource = nullptr) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    switch(type.kind()) {
        case kind::boolean: dest.set_value<meta::field_type_traits<kind::boolean>::runtime_type>(offset, src.read<meta::field_type_traits<kind::boolean>::runtime_type>(odr, false)); break;
        case kind::int1: dest.set_value<meta::field_type_traits<kind::int1>::runtime_type>(offset, src.read<meta::field_type_traits<kind::int1>::runtime_type>(odr, false)); break;
        case kind::int2: dest.set_value<meta::field_type_traits<kind::int2>::runtime_type>(offset, src.read<meta::field_type_traits<kind::int2>::runtime_type>(odr, false)); break;
        case kind::int4: dest.set_value<meta::field_type_traits<kind::int4>::runtime_type>(offset, src.read<meta::field_type_traits<kind::int4>::runtime_type>(odr, false)); break;
        case kind::int8: dest.set_value<meta::field_type_traits<kind::int8>::runtime_type>(offset, src.read<meta::field_type_traits<kind::int8>::runtime_type>(odr, false)); break;
        case kind::float4: dest.set_value<meta::field_type_traits<kind::float4>::runtime_type>(offset, src.read<meta::field_type_traits<kind::float4>::runtime_type>(odr, false)); break;
        case kind::float8: dest.set_value<meta::field_type_traits<kind::float8>::runtime_type>(offset, src.read<meta::field_type_traits<kind::float8>::runtime_type>(odr, false)); break;
        case kind::character: dest.set_value<meta::field_type_traits<kind::character>::runtime_type>(offset, src.read<meta::field_type_traits<kind::character>::runtime_type>(odr, false, resource)); break;
        default:
            fail();
    }
}

/**
 * @brief decode a nullable field's kvs binary representation to a field data
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 * @param dest the record to containing the field
 * @param offset byte offset of the field
 * @param nullity_offset bit offset of the field nullity
 * @param resource the memory resource used to generate text data. nullptr can be passed if no text field is processed.
 */
inline void decode_nullable(
    stream& src,
    meta::field_type const& type,
    coding_spec spec,
    accessor::record_ref dest,
    std::size_t offset,
    std::size_t nullity_offset,
    memory::paged_memory_resource* resource = nullptr
) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    auto flag = src.read<meta::field_type_traits<kind::boolean>::runtime_type>(odr, false);
    BOOST_ASSERT(flag == 0 || flag == 1);  //NOLINT
    bool is_null = flag == 0;
    dest.set_null(nullity_offset, is_null);
    if (! is_null) {
        decode(src, type, spec, dest, offset, resource);
    }
}

/**
 * @brief read kvs binary representation which is not nullable, proceed the stream, and discard the result
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 */
inline void consume_stream(stream& src, meta::field_type const& type, coding_spec spec) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    switch(type.kind()) {
        case kind::boolean: src.read<meta::field_type_traits<kind::boolean>::runtime_type>(odr, true); break;
        case kind::int1: src.read<meta::field_type_traits<kind::int1>::runtime_type>(odr, true); break;
        case kind::int2: src.read<meta::field_type_traits<kind::int2>::runtime_type>(odr, true); break;
        case kind::int4: src.read<meta::field_type_traits<kind::int4>::runtime_type>(odr, true); break;
        case kind::int8: src.read<meta::field_type_traits<kind::int8>::runtime_type>(odr, true); break;
        case kind::float4: src.read<meta::field_type_traits<kind::float4>::runtime_type>(odr, true); break;
        case kind::float8: src.read<meta::field_type_traits<kind::float8>::runtime_type>(odr, true); break;
        case kind::character: src.read<meta::field_type_traits<kind::character>::runtime_type>(odr, true, nullptr); break;
        default:
            fail();
    }
}

/**
 * @brief read kvs binary representation which is nullable, proceed the stream, and discard the result
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 */
inline void consume_stream_nullable(stream& src, meta::field_type const& type, coding_spec spec) {
    using kind = meta::field_type_kind;
    auto odr = spec.ordering();
    auto flag = src.read<meta::field_type_traits<kind::boolean>::runtime_type>(odr, false);
    BOOST_ASSERT(flag == 0 || flag == 1);  //NOLINT
    bool is_null = flag == 0;
    if (! is_null) {
        consume_stream(src, type, spec);
    }
}

}

