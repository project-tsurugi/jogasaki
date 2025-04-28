#pragma once

#include <cstdint>
#include <cstdlib>
#include <iosfwd>
#include <stdexcept>
#include <string>
#include <string_view>

namespace jogasaki::serializer {

/**
 * @brief an exception occurs when the value_input reached broken input.
 */
class value_input_exception : public std::runtime_error {
public:
    /**
     * @brief the reason code of individual erroneous situations.
     */
    enum class reason_code {
        /// @brief reached end of buffer before reading value is completed.
        buffer_underflow,

        /// @brief unrecognized entry type.
        unrecognized_entry_type,

        /// @brief unsupported entry type.
        unsupported_entry_type,

        /// @brief value is out of range.
        value_out_of_range,
    };

    /**
     * @brief creates a new instance.
     * @param reason the reason code
     * @param message the error message
     */
    explicit value_input_exception(reason_code reason, char const* message);

    /**
     * @brief creates a new instance.
     * @param reason the reason code
     * @param message the error message
     */
    explicit value_input_exception(reason_code reason, std::string const& message);

    /**
     * @brief returns the reason code of this exception.
     * @return the reason code
     */
    [[nodiscard]] reason_code reason() const noexcept;

private:
    reason_code reason_;
};

/**
 * @brief raise a new exception for buffer underflow.
 */
[[noreturn]] void throw_buffer_underflow();

/**
 * @brief raise a new exception for unrecognized entry kind.
 * @param header the header byte
 */
[[noreturn]] void throw_unrecognized_entry(std::uint32_t header);

/**
 * @brief raise a new exception for unsupported entry kind.
 * @param header the header byte
 */
[[noreturn]] void throw_unsupported_entry(std::uint32_t header);

/**
 * @brief raise a new exception for extracted 32-bit signed int value is out of range.
 * @param value the extracted value
 */
[[noreturn]] void throw_int32_value_out_of_range(std::int64_t value);

/**
 * @brief raise a new exception for extracted decimal value is out of range.
 * @param nbytes the number of octets
 */
[[noreturn]] void throw_decimal_coefficient_out_of_range(std::size_t nbytes);

/**
 * @brief raise a new exception for extracted size is out of range.
 * @param size the extracted size
 * @param limit the size limit
 */
[[noreturn]] void throw_size_out_of_range(std::uint64_t size, std::uint64_t limit);

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
constexpr std::string_view to_string_view(value_input_exception::reason_code value) noexcept {
    using namespace std::string_view_literals;
    using kind = value_input_exception::reason_code;
    switch (value) {
        case kind::buffer_underflow: return "buffer_underflow"sv;
        case kind::unrecognized_entry_type: return "unrecognized_entry_type"sv;
        case kind::unsupported_entry_type: return "unsupported_entry_type"sv;
        case kind::value_out_of_range: return "value_out_of_range"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, value_input_exception::reason_code value);

} // namespace jogasaki::serializer
