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

#include <cmath>
#include <boost/endian/conversion.hpp>

#include "readable_stream.h"

namespace jogasaki::kvs {

namespace details {

template<std::size_t N>
static inline uint_t<N> key_encode(int_t<N> data, order odr) {
    auto u = type_change<uint_t<N>>(data);
    u ^= SIGN_BIT<N>;
    if (odr != order::ascending) {
        u = ~u;
    }
    return boost::endian::native_to_big(u);
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

}  // namespace details

/**
 * @brief stream to serialize/deserialize kvs key/value data
 */
class writable_stream {
public:
    /**
     * @brief create object with zero capacity. This object doesn't hold data, but can be used to calculate length.
     */
    writable_stream() = default;

    /**
     * @brief create new object
     * @param buffer pointer to buffer that this instance can use
     * @param capacity length of the buffer
     */
    writable_stream(void* buffer, std::size_t capacity);

    /**
     * @brief construct stream using string as its buffer
     * @param s string to use stream buffer
     */
    explicit writable_stream(std::string& s);

    /**
     * @brief write the typed field data respecting order to the stream
     * @tparam T the runtime type of the field
     * @tparam N the number of bits for the written data
     * @param data the data of the field type
     * @param odr the order of the field
     */
    template<class T, std::size_t N = sizeof(T) * bits_per_byte>
    std::enable_if_t<(std::is_integral_v<T> && (N == 8 || N == 16 || N == 32 || N == 64)) ||
        (std::is_floating_point_v<T> && (N == 32 || N == 64)), void> write(T data, order odr) {
        do_write<N>(details::key_encode<N>(data, odr));
    }

    /**
     * @brief write the accessor::text field data respecting order to the stream
     * @tparam T must be accessor::text
     * @param data the text data to be written
     * @param odr the order of the field
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, accessor::text>, void> write(T data, order odr) {
        std::string_view sv{data};
        // for key encoding, we are assuming the text is not so long
        BOOST_ASSERT(sv.length() < 32768); //NOLINT
        details::text_encoding_prefix_type len{static_cast<details::text_encoding_prefix_type>(sv.length())};
        do_write<details::text_encoding_prefix_type_bits>(details::key_encode<details::text_encoding_prefix_type_bits>(len, odr));
        do_write(sv.data(), sv.size(), odr);
    }

    /**
     * @brief write raw data to the stream buffer
     * @details the raw data is written to the stream. Given binary sequence is used
     * and no ordering or type conversion occurs.
     */
    void write(char const* dt, std::size_t sz);

    /**
     * @brief reset the current position
     */
    void reset();

    /**
     * @brief return current length of the stream (# of bytes already written)
     * @return the length of the stream
     */
    [[nodiscard]] std::size_t size() const noexcept;

    /**
     * @brief return the capacity of the stream buffer
     * @return the backing buffer capacity
     */
    [[nodiscard]] std::size_t capacity() const noexcept;

    /**
     * @brief return the beginning pointer to the stream buffer
     * @return the data pointer
     */
    [[nodiscard]] char const* data() const noexcept;

    /**
     * @brief retrieve readable_stream for the buffer owned by this object
     * @return the readable stream
     */
    [[nodiscard]] readable_stream readable() const noexcept;

private:
    char* base_{};
    std::size_t pos_{};
    std::size_t capacity_{};

    template<std::size_t N>
    void do_write(details::uint_t<N> data) {
        auto sz = N/bits_per_byte;
        BOOST_ASSERT(capacity_ == 0 || pos_ + sz <= capacity_);  //NOLINT
        if (capacity_ > 0) {
            std::memcpy(base_+pos_, reinterpret_cast<char*>(&data), sz); //NOLINT
        }
        pos_ += sz;
    }

    void do_write(char const* dt, std::size_t sz, order odr);

};

}

