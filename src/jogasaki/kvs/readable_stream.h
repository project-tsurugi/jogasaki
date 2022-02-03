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

#include <boost/endian/conversion.hpp>

#include "coder.h"

namespace jogasaki::kvs {

namespace details {

template<std::size_t N>
static inline void key_decode(int_t<N>& value, uint_t<N> data, order odr) {
    auto u = boost::endian::big_to_native(data);
    if (odr != order::ascending) {
        u = ~u;
    }
    u ^= SIGN_BIT<N>;
    value = type_change<int_t<N>>(u);
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

/**
 * @brief stream to serialize/deserialize kvs key/value data
 */
class readable_stream {
public:
    /**
     * @brief create object with zero capacity. This object doesn't hold data.
     */
    readable_stream() = default;

    /**
     * @brief create new object
     * @param buffer pointer to buffer that this instance can use
     * @param capacity length of the buffer
     */
    readable_stream(void const* buffer, std::size_t capacity);

    /**
     * @brief construct stream using string as its buffer
     * @param s string to use stream buffer
     */
    explicit readable_stream(std::string& s);

    /**
     * @brief read next uint_t N bits integer in the buffer
     * @param discard specify true if the read should not actually happen.
     */
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

    /**
     * @brief read next integer or floating number in the buffer
     * @param odr the order of the field
     * @param discard specify true if the read should not actually happen.
     */
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

    std::size_t read_text_length(order odr) {
        auto& term = details::get_terminator(odr);
        auto pos = pos_;
        while(pos < capacity_) {
            if(term.equal(base_+pos, capacity_-pos)) {  //NOLINT
                return pos - pos_;
            }
            ++pos;
        }
        return pos - pos_;
    }

    /**
     * @brief read next text in the buffer
     * @param odr the order of the field
     * @param discard specify true if the read should not actually happen.
     * @param resource the resource to allocate the content read
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, accessor::text>, T> read(order odr, bool discard, memory::paged_memory_resource* resource = nullptr) {
        auto len = read_text_length(odr);
        BOOST_ASSERT(pos_ + len <= capacity_);  // NOLINT
        auto pos = pos_;
        pos_ += len + details::text_terminator::byte_size;
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
     * @brief return the rest of the buffer (ranging from current position to end of the buffer)
     * @return the rest of the data
     */
    [[nodiscard]] std::string_view rest() const noexcept;

private:
    char const* base_{};
    std::size_t pos_{};
    std::size_t capacity_{};
};

}

