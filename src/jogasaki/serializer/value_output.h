#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <boost/dynamic_bitset.hpp>

#include <takatori/datetime/date.h>
#include <takatori/datetime/datetime_interval.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/bitset_view.h>
#include <takatori/util/buffer_view.h>

namespace jogasaki::serializer {

using takatori::util::buffer_view;
using takatori::util::bitset_view;
using takatori::util::const_bitset_view;

/**
 * @brief puts `end_of_contents` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_end_of_contents(
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `null` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_null(
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `int` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_int(
        std::int64_t value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `float4` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_float4(
        float value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `float8` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_float8(
        double value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `decimal` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note This may write as `int` entry if th value is in the range of 64-bit signed integer.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_decimal(
        takatori::decimal::triple value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `character` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note The returned std::string_view refers onto the input buffer.
 *      Please escape the returned value before the buffer will be disposed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_character(
        std::string_view value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `octet` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note The returned std::string_view refers onto the input buffer.
 *      Please escape the returned value before the buffer will be disposed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_octet(
        std::string_view value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `bit` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note The returned bitset_view refers onto the input buffer.
 *      Please escape the returned value before the buffer will be disposed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_bit(
        const_bitset_view value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `bit` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note The returned bitset_view refers onto the input buffer.
 *      Please escape the returned value before the buffer will be disposed.
 * @param blocks the bit blocks
 * @param number_of_bits the number of bits to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 * @throws std::out_of_range if `number_of_bits` is too large for `blocks`
 */
bool write_bit(
        std::string_view blocks,
        std::size_t number_of_bits,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `date` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_date(
        takatori::datetime::date value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `time_of_day` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_time_of_day(
        takatori::datetime::time_of_day value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `time_of_day` with timezone offset entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param timezone_offset the timezone offset (in minute) to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_time_of_day_with_offset(
    takatori::datetime::time_of_day value,
    std::int32_t timezone_offset,
    buffer_view::iterator& position,
    buffer_view::const_iterator end);

/**
 * @brief puts `time_point` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_time_point(
        takatori::datetime::time_point value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `time_point` with timezone offset entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param timezone_offset the timezone offset (in minute) to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_time_point_with_offset(
    takatori::datetime::time_point value,
    std::int32_t timezone_offset,
    buffer_view::iterator& position,
    buffer_view::const_iterator end);

/**
 * @brief puts `datetime_interval` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param value the value to write
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_datetime_interval(
        takatori::datetime::datetime_interval value,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `array` entry onto the current position.
 * @details This entry does not its elements:
 *      please retrieve individual values using `write_xxx()` function for each the returned element count.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param size the number of elements in the array, must be less than `2^31` for interoperability
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 * @throws std::out_of_range if size is out of range
 */
bool write_array_begin(
        std::size_t size,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `row` entry onto the current position.
 * @details This entry does not its elements:
 *      please retrieve individual values using `write_xxx()` function for each the returned element count.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param size the number of elements in the row, must be less than `2^31` for interoperability
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 * @throws std::out_of_range if size is out of range
 */
bool write_row_begin(
        std::size_t size,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `blob` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param provider the provider of the blob, see jogasaki::proto::sql::common::LargeObjectProvider
 * @param object_id the id of the blob object
 * @param reference_tag the reference tag of the blob object
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_blob(
        std::uint64_t provider,
        std::uint64_t object_id,
        std::uint64_t reference_tag,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

/**
 * @brief puts `clob` entry onto the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param provider the provider of the clob, see jogasaki::proto::sql::common::LargeObjectProvider
 * @param object_id the id of the clob object
 * @param reference_tag the reference tag of the clob object
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return true the operation successfully completed
 * @return false the remaining buffer is too short to write contents
 */
bool write_clob(
        std::uint64_t provider,
        std::uint64_t object_id,
        std::uint64_t reference_tag,
        buffer_view::iterator& position,
        buffer_view::const_iterator end);

} // namespace jogasaki::serializer
