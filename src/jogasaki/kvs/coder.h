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

namespace jogasaki::kvs {

using takatori::util::fail;

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
static inline uint_t<N> key_encode(int_t<N> data, bool ascending) {
    auto u = type_change<uint_t<N>>(data);
    u ^= SIGN_BIT<N>;
    if (!ascending) {
        u = ~u;
    }
    return boost::endian::native_to_big(u);
}

template<std::size_t N>
static inline void key_decode(int_t<N>& value, uint_t<N> data, bool ascending) {
    auto u = boost::endian::big_to_native(data);
    if (!ascending) {
        u = ~u;
    }
    u ^= SIGN_BIT<N>;
    value = type_change<int_t<N>>(u);
}

// encode and decode logic for double with considering the key is ascending or not
template<std::size_t N>
static inline uint_t<N> key_encode(float_t<N> data, bool ascending) {
    auto u = type_change<uint_t<N>>(std::isnan(data) ? std::numeric_limits<float_t<N>>::quiet_NaN() : data);
    if ((u & SIGN_BIT<N>) == 0) {
        u ^= SIGN_BIT<N>;
    } else {
        u = ~u;
    }
    if (!ascending) {
        u = ~u;
    }
    return boost::endian::native_to_big(u);
}

template<std::size_t N>
static inline void key_decode(float_t<N>& value, uint_t<N> data, bool ascending) {
    auto u = boost::endian::big_to_native(data);
    if (!ascending) {
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
     * @brief create empty object
     */
    stream() = default;

    /**
     * @brief create new object
     * @param buffer pointer to buffer that this instance can use
     * @param capacity length of the buffer
     */
    stream(char* buffer, std::size_t capacity) : base_(buffer), capacity_(capacity) {}

    /**
     * @brief construct stream using string as its buffer (mainly for testing purpose)
     * @param s string to use stream buffer
     */
    explicit stream(std::string& s) : stream(s.data(), s.capacity()) {}

    template<std::size_t N>
    void do_write(details::uint_t<N> data) {
        std::memcpy(base_+pos_, reinterpret_cast<char*>(&data), N/bits_per_byte); //NOLINT
        pos_ += N/bits_per_byte;
    }

    template<class T, std::size_t N = sizeof(T) * bits_per_byte>
    std::enable_if_t<(std::is_integral_v<T> || std::is_floating_point_v<T>) && (N == 32 || N == 64), void> write(T data, bool ascending = true) {
        do_write<N>(details::key_encode<N>(data, ascending));
    }

    void do_write(char const* dt, std::size_t sz, bool ascending) {
        assert(pos_ + sz <= capacity_);  // NOLINT
        if (sz > 0) {
            if (ascending) {
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
    std::enable_if_t<std::is_same_v<T, accessor::text>, void> write(T data, bool ascending = true) {
        std::string_view sv{data};
        // for key encoding, we are assuming the text is not so long
        assert(sv.length() < 32768); //NOLINT
        details::text_encoding_prefix_type len{static_cast<details::text_encoding_prefix_type>(sv.length())};
        do_write<details::text_encoding_prefix_type_bits>(details::key_encode<details::text_encoding_prefix_type_bits>(len, ascending));
        do_write(sv.data(), sv.size(), ascending);
    }

    template<std::size_t N>
    details::uint_t<N> do_read() {
        auto sz = N/bits_per_byte;
        assert(pos_ + sz <= capacity_);  // NOLINT
        details::uint_t<N> ret{};
        std::memcpy(&ret, base_+pos_, sz); //NOLINT
        pos_ += sz;
        return ret;
    }

    template<class T, std::size_t N = sizeof(T) * bits_per_byte>
    std::enable_if_t<(std::is_integral_v<T> || std::is_floating_point_v<T>) && (N == 16 || N == 32 || N == 64), T> read(bool ascending = true) {
        T value{};
        details::key_decode<N>(value, do_read<N>(), ascending);
        return value;
    }

    template<class T>
    std::enable_if_t<std::is_same_v<T, accessor::text>, T> read(memory::paged_memory_resource* resource = nullptr, bool ascending = true) {
        assert(pos_ + details::text_encoding_prefix_type_bits / bits_per_byte <= capacity_);  // NOLINT
        auto l = read<details::text_encoding_prefix_type>(ascending);
        assert(l >= 0); //NOLINT
        auto len = static_cast<std::size_t>(l);
        assert(pos_ + len <= capacity_);  // NOLINT
        if (len > 0) {
            auto p = static_cast<char*>(resource->allocate(len));
            if (ascending) {
                std::memcpy(p, base_ + pos_, len);  // NOLINT
            } else {
                for (std::size_t i = 0; i < len; ++i) {
                    *(p + i) = ~(*(base_ + pos_ + i));  // NOLINT
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
 * @brief encode a field data to kvs binary representation
 * @param ref the record containing data to encode
 * @param offset byte offset of the field containing data to encode
 * @param type the type of the field
 * @param dest the stream where the encoded data is written
 */
inline void encode(accessor::record_ref ref, std::size_t offset, meta::field_type const& type, stream& dest) {
    using kind = meta::field_type_kind;
    switch(type.kind()) {
        case kind::int4: dest.write<meta::field_type_traits<kind::int4>::runtime_type>(ref.get_value<meta::field_type_traits<kind::int4>::runtime_type>(offset)); break;
        case kind::int8: dest.write<meta::field_type_traits<kind::int8>::runtime_type>(ref.get_value<meta::field_type_traits<kind::int8>::runtime_type>(offset)); break;
        case kind::float4: dest.write<meta::field_type_traits<kind::float4>::runtime_type>(ref.get_value<meta::field_type_traits<kind::float4>::runtime_type>(offset)); break;
        case kind::float8: dest.write<meta::field_type_traits<kind::float8>::runtime_type>(ref.get_value<meta::field_type_traits<kind::float8>::runtime_type>(offset)); break;
        case kind::character: dest.write<meta::field_type_traits<kind::character>::runtime_type>(ref.get_value<meta::field_type_traits<kind::character>::runtime_type>(offset)); break;
        default:
            fail();
    }
}

/**
 * @brief decode kvs binary representation to a field data
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param ref the record to containing the field
 * @param offset byte offset of the field
 * @param resource the memory resource used to generate text data. nullptr can be passed if no text field is processed.
 */
inline void decode(stream& src, meta::field_type const& type, accessor::record_ref ref, std::size_t offset, memory::paged_memory_resource* resource = nullptr) {
    using kind = meta::field_type_kind;
    switch(type.kind()) {
        case kind::int4: ref.set_value<meta::field_type_traits<kind::int4>::runtime_type>(offset, src.read<meta::field_type_traits<kind::int4>::runtime_type>()); break;
        case kind::int8: ref.set_value<meta::field_type_traits<kind::int8>::runtime_type>(offset, src.read<meta::field_type_traits<kind::int8>::runtime_type>()); break;
        case kind::float4: ref.set_value<meta::field_type_traits<kind::float4>::runtime_type>(offset, src.read<meta::field_type_traits<kind::float4>::runtime_type>()); break;
        case kind::float8: ref.set_value<meta::field_type_traits<kind::float8>::runtime_type>(offset, src.read<meta::field_type_traits<kind::float8>::runtime_type>()); break;
        case kind::character: ref.set_value<meta::field_type_traits<kind::character>::runtime_type>(offset, src.read<meta::field_type_traits<kind::character>::runtime_type>(resource)); break;
        default:
            fail();
    }
}

}

