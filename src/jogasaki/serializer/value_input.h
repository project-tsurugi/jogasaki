#pragma once

#include <cstdint>

#include <boost/dynamic_bitset.hpp>

#include <takatori/decimal/triple.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/datetime/datetime_interval.h>

#include <takatori/util/bitset_view.h>
#include <takatori/util/buffer_view.h>

#include "entry_type.h"
#include "value_input_exception.h"

namespace jogasaki::serializer {

using takatori::util::buffer_view;
using takatori::util::bitset_view;
using takatori::util::const_bitset_view;

/**
 * @brief returns the entry type of the iterator position.
 * @details this operation does not advance the buffer iterator.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the current entry type
 * @return entry_type::buffer_underflow if there are no more entries in this buffer
 * @throw value_input_exception if the buffer is empty
 * @throw value_input_exception if the input entry type is not supported
 */
entry_type peek_type(buffer_view::const_iterator position, buffer_view::const_iterator end);

/**
 * @brief just consumes `end_of_contents` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @throws std::runtime_error if the entry is not expected type
 * @see peek_type()
 */
void read_end_of_contents(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief just consumes `null` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @throws std::runtime_error if the entry is not expected type
 * @see peek_type()
 */
void read_null(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `int` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
std::int64_t read_int(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief peaks `float4` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
float read_float4(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `float8` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
double read_float8(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `decimal` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note This also recognizes `int` entries because `decimal` values sometimes encoded as `int` value.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
takatori::decimal::triple read_decimal(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `character` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note The returned std::string_view refers onto the input buffer.
 *      Please escape the returned value before the buffer will be disposed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
std::string_view read_character(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `octet` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note The returned std::string_view refers onto the input buffer.
 *      Please escape the returned value before the buffer will be disposed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
std::string_view read_octet(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `bit` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @note The returned bitset_view refers onto the input buffer.
 *      Please escape the returned value before the buffer will be disposed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
const_bitset_view read_bit(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `date` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
takatori::datetime::date read_date(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `time_of_day` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
takatori::datetime::time_of_day read_time_of_day(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `time_point` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
takatori::datetime::time_point read_time_point(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `datetime_interval` entry on the current position.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the retrieved value
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
takatori::datetime::datetime_interval read_datetime_interval(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `array` entry on the current position.
 * @details This entry does not its elements:
 *      please retrieve individual values using `read_xxx()` function for each the returned element count.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the consequent number of elements in the array
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
std::size_t read_array_begin(buffer_view::const_iterator& position, buffer_view::const_iterator end);

/**
 * @brief retrieves `row` entry on the current position.
 * @details This entry does not its elements:
 *      please retrieve individual values using `read_xxx()` function for each the returned element count.
 * @details This operation will advance the buffer iterator to the next entry, only if it is successfully completed.
 * @param position the buffer content iterator
 * @param end the buffer ending position
 * @return the consequent number of elements in the row
 * @throws std::runtime_error if the entry is not expected type
 * @throws value_input_exception if the encoded value is not valid
 * @see peek_type()
 */
std::size_t read_row_begin(buffer_view::const_iterator& position, buffer_view::const_iterator end);

// FIXME: impl blob, clob

} // namespace jogasaki::serializer
