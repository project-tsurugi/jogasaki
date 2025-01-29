#pragma once

#include <cstdint>

#include <vector>

#include "value_output.h"

#include <takatori/util/assertion.h>

namespace jogasaki::serializer {

using takatori::util::buffer_view;

/**
 * @brief writes value entries into a backing writer.
 * @tparam Writer the backed writer type, should declare `T::write(char const* data, size_type size) -> result_type`
 * @tparam Size the size type represents the number of bytes to write
 */
template<class Writer, class Size = std::uint32_t>
class value_writer {
public:
    /// @brief the backed writer type.
    using writer_type = Writer;

    /// @brief Size the size type represents the number of bytes to write.
    using size_type = Size;
    
    /// @brief the result type of `T::write(char const*, size_type)`.
    using result_type = decltype(
            std::declval<writer_type>().write(
                    std::declval<char const*>(),
                    std::declval<size_type>()));

    /**
     * @brief creates a new instance.
     * @param writer the destination writer
     */
    explicit value_writer(writer_type& writer) noexcept :
        writer_ { std::addressof(writer) }
    {}

    /**
     * @brief puts `end_of_contents` entry onto the current position.
     */
    result_type write_end_of_contents() {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_end_of_contents(iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `null` entry onto the current position.
     */
    result_type write_null() {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_null(iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `int` entry onto the current position.
     */
    result_type write_int(std::int64_t value) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_int(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `float4` entry onto the current position.
     * @param value the value to write
     */
    result_type write_float4(float value) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_float4(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `float8` entry onto the current position.
     * @param value the value to write
     */
    result_type write_float8(double value) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_float8(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `decimal` entry onto the current position.
     * @param value the value to write
     */
    result_type write_decimal(takatori::decimal::triple value) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_decimal(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `character` entry onto the current position.
     * @param value the value to write
     */
    result_type write_character(std::string_view value) {
        auto buf = buffer(value.size() + 10);
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_character(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `octet` entry onto the current position.
     * @param value the value to write
     */
    result_type write_octet(std::string_view value) {
        auto buf = buffer(value.size() + 10);
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_octet(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `bit` entry onto the current position.
     * @param value the value to write
     */
    result_type write_bit(const_bitset_view value) {
        auto buf = buffer(value.block_size() + 10);
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_bit(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `bit` entry onto the current position.
     * @param blocks the bit blocks
     * @param number_of_bits the number of bits to write
     */
    result_type write_bit(std::string_view blocks, std::size_t number_of_bits) {
        auto buf = buffer(blocks.size() + 16);
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_bit(blocks, number_of_bits, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `date` entry onto the current position.
     * @param value the value to write
     */
    result_type write_date(takatori::datetime::date value) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_date(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `time_of_day` entry onto the current position.
     * @param value the value to write
     */
    result_type write_time_of_day(takatori::datetime::time_of_day value) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_time_of_day(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `time_of_day` with offset entry onto the current position.
     * @param value the value to write
     * @param timezone_offset the timezone offset in minute
     */
    result_type write_time_of_day_with_offset(takatori::datetime::time_of_day value, std::int32_t timezone_offset) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_time_of_day_with_offset(value, timezone_offset, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `time_point` entry onto the current position.
     * @param value the value to write
     */
    result_type write_time_point(takatori::datetime::time_point value) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_time_point(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `time_point` with offset entry onto the current position.
     * @param value the value to write
     * @param timezone_offset the timezone offset in minute
     */
    result_type write_time_point_with_offset(takatori::datetime::time_point value, std::int32_t timezone_offset) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_time_point_with_offset(value, timezone_offset, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `datetime_interval` entry onto the current position.
     * @param value the value to write
     */
    result_type write_datetime_interval(takatori::datetime::datetime_interval value) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_datetime_interval(value, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `array` entry onto the current position.
     * @param size the number of elements in the array, must be less than `2^31` for interoperability
     */
    result_type write_array_begin(std::size_t size) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_array_begin(size, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `row` entry onto the current position.
     * @param size the number of elements in the row, must be less than `2^31` for interoperability
     */
    result_type write_row_begin(std::size_t size) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_row_begin(size, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `blob` entry onto the current position.
     * @param provider the provider of the blob, see jogasaki::proto::sql::common::LargeObjectProvider
     * @param object_id the id of the blob object
     */
    result_type write_blob(std::uint64_t provider, std::uint64_t object_id) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_blob(provider, object_id, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

    /**
     * @brief puts `clob` entry onto the current position.
     * @param provider the provider of the clob, see jogasaki::proto::sql::common::LargeObjectProvider
     * @param object_id the id of the clob object
     */
    result_type write_clob(std::uint64_t provider, std::uint64_t object_id) {
        auto buf = buffer();
        auto *iter = buf.begin();
        auto ret = ::jogasaki::serializer::write_clob(provider, object_id, iter, buf.end());
        BOOST_ASSERT(ret); // NOLINT
        (void) ret;

        auto write_size = static_cast<size_type>(std::distance(buf.begin(), iter));
        return writer_->write(buf.data(), write_size);
    }

private:
    std::vector<buffer_view::value_type> buffer_;
    writer_type* writer_;

    [[nodiscard]] buffer_view buffer(std::size_t reserve = 1024) {
        if (buffer_.size() < reserve) {
            buffer_.resize(reserve);
        }
        return buffer_view { buffer_.data(), buffer_.size() };
    }
};

} // namespace jogasaki::serializer
