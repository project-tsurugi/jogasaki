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
#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <boost/endian/conversion.hpp>
#include <glog/logging.h>

#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/coding_context.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/octet_field_option.h>
#include <jogasaki/status.h>

namespace jogasaki::kvs {

using takatori::util::throw_exception;

static constexpr char padding_character = '\x20';
static constexpr char padding_octet = '\x00';

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

template<std::size_t N>
static inline uint_t<N> key_encode(uint_t<N> data, order odr) {
    auto u = data;
    if (odr != order::ascending) {
        u = ~u;
    }
    return boost::endian::native_to_big(u);
}

// encode and decode logic for double with considering the key is ascending or not
template<std::size_t N>
static inline uint_t<N> key_encode(float_t<N> data, order odr) {
    float_t<N> d{data};
    if(global::config_pool()->normalize_float()) { // testing can skip normalization
        d = std::isnan(data) ? std::numeric_limits<float_t<N>>::quiet_NaN() : data;
        // eliminate -0.0
        d = d == static_cast<float_t<N>>(0.0) ? static_cast<float_t<N>>(0.0) : d;
    }
    auto u = type_change<uint_t<N>>(d);
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
     * @param ignore_overflow when set to true, writing longer data than capacity is ignored,
     * otherwise the behavior is UB
     */
    writable_stream(void* buffer, std::size_t capacity, bool ignore_overflow = false);

    /**
     * @brief construct stream using string as its buffer
     * @param s string to use stream buffer
     * @param ignore_overflow when set to true, writing longer data than capacity is ignored,
     * otherwise the behavior is UB
     */
    explicit writable_stream(std::string& s, bool ignore_overflow = false);

    /**
     * @brief write the typed field data respecting order to the stream
     * @tparam T the runtime type of the field
     * @tparam N the number of bits for the written data
     * @param data the data of the field type
     * @param odr the order of the field
     */
    template<class T, std::size_t N = sizeof(T) * bits_per_byte>
    std::enable_if_t<(std::is_integral_v<T> && (N == 8 || N == 16 || N == 32 || N == 64)) ||
        (std::is_floating_point_v<T> && (N == 32 || N == 64)), status> write(T data, order odr) {
        do_write<N>(details::key_encode<N>(data, odr));
        return status::ok;
    }

    bool validate_text(std::string_view sv) {
        for(std::size_t i=0,n=sv.size(); i<n; ++i) {
            if(sv[i] == 0) {
                VLOG_LP(log_error) << "an invalid octet appears in the character field data position:" << i << " data length:" << n;
                return false;
            }
        }
        return true;
    }

    /**
     * @brief write the accessor::text field data respecting order to the stream
     * @tparam T must be accessor::text
     * @param data the text data to be written
     * @param odr the order of the field
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, accessor::text>, status> write(T data, order odr, meta::character_field_option const& option, bool is_key = true) {
        std::size_t max_len = option.length_
            ? *option.length_
            : (option.varying_ ? (is_key ? character_type_max_length_for_key : character_type_max_length_for_value)
                               : character_type_default_length);

        std::string_view sv{data};
        auto sz = sv.length();
        if((! context_ || context_->coding_for_write()) && max_len < sz) {
            VLOG_LP(log_error) << "insufficient storage to store field data. storage max:" << max_len << " data length:" << sz;
            return status::err_insufficient_field_storage;
        }
        if(! validate_text(sv)) {
            return status::err_invalid_runtime_value;
        }
        do_write(sv.data(), sz, odr);
        if((! context_ || context_->coding_for_write()) && ! option.varying_) {
            // padding chars
            if(sv.size() < max_len) {
                do_write(padding_character, max_len-sv.size(), odr);
            }
        }
        auto& term = details::get_terminator(odr);
        if(auto res = write(term.data(), term.size()); res != status::ok) {
            return res;
        }
        return status::ok;
    }

    /**
     * @brief write the accessor::binary field data respecting order to the stream
     * @tparam T must be accessor::binary
     * @param data the binary data to be written
     * @param odr the order of the field
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, accessor::binary>, status> write(T data, order odr, meta::octet_field_option const& option, bool is_key = true) {
        std::size_t max_len = option.length_.has_value()
            ? *option.length_
            : (option.varying_ ? (is_key ? octet_type_max_length_for_key : octet_type_max_length_for_value)
                               : octet_type_default_length);
        std::string_view sv{data};
        auto sz = sv.length();
        if((! context_ || context_->coding_for_write()) && max_len < sz) {
            VLOG_LP(log_error) << "insufficient storage to store field data. storage max:" << max_len << " data length:" << sz;
            return status::err_insufficient_field_storage;
        }
        if(option.varying_) {
            auto len = static_cast<details::binary_encoding_prefix_type>(sz);
            do_write<details::binary_encoding_prefix_type_bits>(details::key_encode<details::binary_encoding_prefix_type_bits>(len, odr));
        }
        do_write(sv.data(), sz, odr);
        if((! context_ || context_->coding_for_write()) && ! option.varying_) {
            // padding
            if(sz < max_len) {
                do_write(padding_octet, max_len-sz, odr);
            }
        }
        return status::ok;
    }

    /**
     * @brief write the date field data respecting order to the stream
     * @tparam T the runtime type of the field
     * @param data the data of the field type
     * @param odr the order of the field
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::date>>, status> write(T data, order odr) {
        // date is represented as int8
        write<std::int64_t>(data.days_since_epoch(), odr);
        return status::ok;
    }

    /**
     * @brief write the time_of_day field data respecting order to the stream
     * @tparam T the runtime type of the field
     * @param data the data of the field type
     * @param odr the order of the field
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::time_of_day>>, status>
    write(T data, order odr) {
        // time_of_day is represented as int8
        write<std::int64_t>(data.time_since_epoch().count(), odr);
        return status::ok;
    }

    /**
     * @brief write the time_point field data respecting order to the stream
     * @tparam T the runtime type of the field
     * @param data the data of the field type
     * @param odr the order of the field
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::time_point>>, status>
    write(T data, order odr) {
        // time_point is represented as int8 (seconds since epoch) + int4 (subseconds in nano)
        write<std::int64_t>(data.seconds_since_epoch().count(), odr);
        write<std::int32_t>(data.subsecond().count(), odr);
        return status::ok;
    }

    /**
     * @brief write the decimal field data respecting order to the stream
     * @tparam T the runtime type of the field
     * @param data the data of the field type
     * @param odr the order of the field
     * @param option the field option
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::decimal>>, status>
    write(T data, order odr, meta::decimal_field_option const& option) {
        return do_write(data, odr, option);
    }

    /**
     * @brief write the blob field data respecting order to the stream
     * @tparam T the runtime type of the field
     * @param data the data of the field type
     * @param odr the order of the field
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::blob>>, status> write(T data, order odr) {
        // only object id part is needed for kvs data
        write<std::uint64_t>(data.object_id(), odr);
        return status::ok;
    }

    /**
     * @brief write the clob field data respecting order to the stream
     * @tparam T the runtime type of the field
     * @param data the data of the field type
     * @param odr the order of the field
     */
    template<class T>
    std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::clob>>, status> write(T data, order odr) {
        // only object id part is needed for kvs data
        write<std::uint64_t>(data.object_id(), odr);
        return status::ok;
    }

    /**
     * @brief write raw data to the stream buffer
     * @details the raw data is written to the stream. Given binary sequence is used
     * and no ordering or type conversion occurs.
     */
    status write(char const* dt, std::size_t sz);

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

    /**
     * @brief setter of the ignore overflow flag
     * @details when set to true, writing longer data than capacity is ignored, otherwise the behavior is UB
     */
    void ignore_overflow(bool arg) noexcept;

    /**
     * @brief coding context setter
     * @details context information during write is stored in the context
     */
    void set_context(coding_context& arg) noexcept {
        context_ = std::addressof(arg);
    }

private:
    char* base_{};
    std::size_t pos_{};
    std::size_t capacity_{};
    bool ignore_overflow_{false};
    coding_context* context_{};

    template<std::size_t N>
    void do_write(details::uint_t<N> data) {
        auto sz = N/bits_per_byte;
        if (pos_ + sz > capacity_) {
            if(! ignore_overflow_) {
                throw_exception(std::logic_error{""});
            }
        } else {
            std::memcpy(base_+pos_, reinterpret_cast<char*>(&data), sz); //NOLINT
        }
        pos_ += sz;
    }

    void do_write(char const* dt, std::size_t sz, order odr);
    void do_write(char ch, std::size_t sz, order odr);
    status do_write(runtime_t<meta::field_type_kind::decimal> data, order odr, meta::decimal_field_option const& option);
    void write_decimal(std::int8_t sign, std::uint64_t lo, std::uint64_t hi, std::size_t sz, order odr);
};

}

