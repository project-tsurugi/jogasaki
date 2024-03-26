#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

#include <takatori/util/buffer_view.h>

/**
 * @brief serialize/deserialize integers using base128 variant.
 * @details
 *      base128 variant is differ from the original base128 as following:
 *
 *      * each groups ordered as little-endian
 *      * the 9-th group allocates 8-bits
 */
namespace jogasaki::serializer::base128v {

using size_type = std::size_t;
using takatori::util::buffer_view;

/**
 * @brief compute the base128v encoded size.
 * @param value the target value
 * @return the encoded size in bytes
 */
[[nodiscard]] constexpr size_type size_unsigned(std::uint64_t value) noexcept {
    for (std::uint64_t i = 1; i <= 8; ++i) {
        if (value < (1ULL << (i * 7))) {
            return i;
        }
    }
    return 9;
}

/**
 * @brief writes the value as base128 variant into the buffer.
 * @details
 *      This advances the `iterator` argument only if the operation was succeeded.
 *      If remaining buffer is not enough, this will do nothing and return `false`.
 * @param value the value to write
 * @param iterator [INOUT] the buffer iterator
 * @param end  the buffer limit
 * @return true if the encoded value is successfully written to the buffer
 * @return false if remaining buffer is not enough
 */
bool write_unsigned(
        std::uint64_t value,
        buffer_view::iterator& iterator,
        buffer_view::const_iterator end) noexcept;

/**
 * @brief reads a value encoded by base128 variant from the buffer.
 * @details
 *      This advances the `iterator` argument only if the operation was succeeded.
 *      If the encoded value is wrong, this will do nothing and return an `empty` value.
 * @param iterator [INOUT] the buffer iterator
 * @param end  the buffer limit
 * @return an encoded value if this successfully extracted the value from the buffer
 * @return empty if the encoded value is wrong
 */
std::optional<std::uint64_t> read_unsigned(
        buffer_view::iterator& iterator,
        buffer_view::const_iterator end) noexcept;

/// @copydoc read_unsigned()
std::optional<std::uint64_t> read_unsigned(
        buffer_view::const_iterator& iterator,
        buffer_view::const_iterator end) noexcept;

/// @copydoc size_unsigned()
[[nodiscard]] size_type size_signed(std::int64_t value);

/// @copydoc write_unsigned()
bool write_signed(
        std::int64_t value,
        buffer_view::iterator& iterator,
        buffer_view::const_iterator end);

/// @copydoc read_unsigned()
std::optional<std::int64_t> read_signed(
        buffer_view::iterator& iterator,
        buffer_view::const_iterator end);

/// @copydoc read_unsigned()
std::optional<std::int64_t> read_signed(
        buffer_view::const_iterator& iterator,
        buffer_view::const_iterator end);

} // namespace jogasaki::serializer::base128v
