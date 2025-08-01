/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <takatori/util/exception.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/constants.h>
#include <jogasaki/data/any.h>
#include <jogasaki/kvs/coding_context.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/status.h>

namespace jogasaki::kvs {

using takatori::util::throw_exception;

class writable_stream;
class readable_stream;

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
    constexpr coding_spec(
        bool is_key,
        order order
    ) noexcept :
        is_key_(is_key),
        order_(order)
    {}

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

using binary_encoding_prefix_type = std::uint32_t;
constexpr static std::size_t binary_encoding_prefix_type_bits = sizeof(std::uint32_t) * bits_per_byte;

class text_terminator {
public:
    constexpr static size_t byte_size = 4;

    constexpr explicit text_terminator(order odr) {
        for(char & ch : buf_) {  //NOLINT
            ch = static_cast<char>(odr == kvs::order::ascending ? 0 : -1);
        }
    }

    [[nodiscard]] char const* data() const noexcept {
        return std::addressof(buf_[0]); //NOLINT
    }

    bool equal(char const* s, std::size_t safe_len) const {
        if(!(byte_size <= safe_len)) throw_exception(std::domain_error{"buffer over-read"});
        (void)safe_len;
        for(std::size_t i=0; i < byte_size; ++i) {
            if(s[i] != buf_[i]) return false; //NOLINT
        }
        return true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return byte_size;
    }

private:
    union {
        char buf_[byte_size] = {};  //NOLINT
    };
};

constexpr static text_terminator terminator_asc{order::ascending};
constexpr static text_terminator terminator_desc{order::descending};
constexpr static text_terminator terminator_undef{order::undefined};
inline static text_terminator const& get_terminator(order odr) noexcept {
    if(odr == order::ascending) return terminator_asc;
    if(odr == order::descending) return terminator_desc;
    return terminator_undef;
}

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

} // namespace details

/**
 * @brief encode a non-nullable field data to kvs binary representation
 * @param src the record containing data to encode
 * @param offset byte offset of the field containing data to encode
 * @param type the type of the field
 * @param spec the coding spec for the encoded field
 * @param ctx the context of the encoding/decoding
 * @param dest the stream where the encoded data is written
 * @return status::ok when successful
 * @return status::err_data_corruption if encoded data is not valid
 * @return status::err_expression_evaluation_failure if encoding requires inexact operation
 * @return any error otherwise. When error occurs, write might have happened partially,
 * so the destination stream should be reset or discarded.
 */
status encode(accessor::record_ref src,
    std::size_t offset,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    writable_stream& dest);

/**
 * @brief encode a nullable field data to kvs binary representation
 * @param src the record containing data to encode
 * @param offset byte offset of the field containing data to encode
 * @param nullity_offset bit offset of the field nullity
 * @param type the type of the field
 * @param spec the coding spec for the encoded field
 * @param ctx the context of the encoding/decoding
 * @param dest the stream where the encoded data is written
 * @return status::ok when successful
 * @return status::err_data_corruption if encoded data is not valid
 * @return status::err_expression_evaluation_failure if encoding requires inexact operation
 * @return any error otherwise. When error occurs, write might have happened partially,
 * so the destination stream should be reset or discarded.
 */
status encode_nullable(
    accessor::record_ref src,
    std::size_t offset,
    std::size_t nullity_offset,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    writable_stream& dest
);

/**
 * @brief encode a non-nullable field data to kvs binary representation
 * @param src the source data to encode
 * @param type the type of the field
 * @param spec the coding spec for the encoded field
 * @param ctx the context of the encoding/decoding
 * @param dest the stream where the encoded data is written
 * @return status::ok when successful
 * @return status::err_data_corruption if encoded data is not valid
 * @return status::err_expression_evaluation_failure if encoding requires inexact operation
 * @return any error otherwise. When error occurs, write might have happened partially,
 * so the destination stream should be reset or discarded.
 */
status encode(data::any const& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    writable_stream& dest);

/**
 * @brief encode a nullable field data to kvs binary representation
 * @param src the source data to encode
 * @param type the type of the field
 * @param spec the coding spec for the encoded field
 * @param ctx the context of the encoding/decoding
 * @param dest the stream where the encoded data is written
 * @return status::ok when successful
 * @return status::err_data_corruption if encoded data is not valid
 * @return status::err_expression_evaluation_failure if encoding requires inexact operation
 * @return any error otherwise. When error occurs, write might have happened partially,
 * so the destination stream should be reset or discarded.
 */
status encode_nullable(
    data::any const& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    writable_stream& dest
);

/**
 * @brief decode a non-nullable field's kvs binary representation to a field data
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 * @param ctx the context of the encoding/decoding
 * @param dest the any container for the result value
 * @param resource the memory resource used to generate text data. nullptr can be passed if no text field is processed.
 * @return status::ok when successful
 * @return status::err_data_corruption if decoded data is not valid
 * @return any error otherwise. When error occurs, decode might have happened partially,
 * so the read stream state and destination should be reset or discarded.
 */
status decode(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    data::any& dest,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief decode a non-nullable field's kvs binary representation to a field data
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 * @param ctx the context of the encoding/decoding
 * @param dest the record to containing the field
 * @param offset byte offset of the field
 * @param nullity_offset bit offset of the field nullity
 * @param resource the memory resource used to generate text data. nullptr can be passed if no text field is processed.
 * @return status::ok when successful
 * @return status::err_data_corruption if decoded data is not valid
 * @return any error otherwise. When error occurs, decode might have happened partially,
 * so the read stream state and destination should be reset or discarded.
 */
status decode(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    accessor::record_ref dest,
    std::size_t offset,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief decode a nullable field's kvs binary representation to a field data
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 * @param ctx the context of the encoding/decoding
 * @param dest the record to containing the field
 * @param offset byte offset of the field
 * @param nullity_offset bit offset of the field nullity
 * @param resource the memory resource used to generate text data. nullptr can be passed if no text field is processed.
 * @return status::ok when successful
 * @return status::err_data_corruption if decoded data is not valid
 * @return any error otherwise. When error occurs, decode might have happened partially,
 * so the read stream state and destination should be reset or discarded.
 */
status decode_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    accessor::record_ref dest,
    std::size_t offset,
    std::size_t nullity_offset,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief decode a nullable field's kvs binary representation to a field data
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 * @param ctx the context of the encoding/decoding
 * @param dest the any container for the result value
 * @param resource the memory resource used to generate text data. nullptr can be passed if no text field is processed.
 * @return status::ok when successful
 * @return status::err_data_corruption if decoded data is not valid
 * @return any error otherwise. When error occurs, decode might have happened partially,
 * so the read stream state and destination should be reset or discarded.
 */
status decode_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx,
    data::any& dest,
    memory::paged_memory_resource* resource = nullptr
);

/**
 * @brief read kvs binary representation which is not nullable, proceed the stream, and discard the result
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 * @param ctx the context of the encoding/decoding
 * @return status::ok when successful
 * @return status::err_data_corruption if decoded data is not valid
 * @return any error otherwise. When error occurs, decode might have happened partially,
 * so the read stream state should be reset or discarded.
 */
status consume_stream(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx);

/**
 * @brief read kvs binary representation which is nullable, proceed the stream, and discard the result
 * @param src the stream where the encoded data is read
 * @param type the type of the field that holds decoded data
 * @param spec the coding spec for the decoded field
 * @param ctx the context of the encoding/decoding
 * @return status::ok when successful
 * @return status::err_data_corruption if decoded data is not valid
 * @return any error otherwise. When error occurs, decode might have happened partially,
 * so the read stream state should be reset or discarded.
 */
status consume_stream_nullable(
    readable_stream& src,
    meta::field_type const& type,
    coding_spec spec,
    coding_context& ctx);

}

