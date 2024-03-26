#include <cstddef>
#include <cstdint>
#include <limits>

#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/serializer/value_input_exception.h>

#include "details/value_io_constants.h"

namespace jogasaki::serializer {

using namespace details;

using ::takatori::util::string_builder;
using ::takatori::util::throw_exception;

value_input_exception::value_input_exception(value_input_exception::reason_code reason, char const* message) :
        std::runtime_error { message },
        reason_ { reason }
{}

value_input_exception::value_input_exception(value_input_exception::reason_code reason, std::string const& message) :
        std::runtime_error { message },
        reason_ { reason }
{}

value_input_exception::reason_code value_input_exception::reason() const noexcept {
    return reason_;
}

std::ostream& operator<<(std::ostream& out, value_input_exception::reason_code value) {
    return out << to_string_view(value);
}


void throw_buffer_underflow() {
    throw_exception(value_input_exception {
            value_input_exception::reason_code::buffer_underflow,
            "input buffer is underflow",
    });
}

void throw_unrecognized_entry(std::uint32_t header) {
    throw_exception(value_input_exception {
            value_input_exception::reason_code::unrecognized_entry_type,
            string_builder {}
                    << "unrecognized entry type: " << header
                    << string_builder::to_string,
    });
}

void throw_unsupported_entry(std::uint32_t header) {
    throw_exception(value_input_exception {
            value_input_exception::reason_code::unsupported_entry_type,
            string_builder {}
                    << "unsupported entry type: " << header
                    << string_builder::to_string,
    });
}

void throw_int32_value_out_of_range(std::int64_t value) {
    throw_exception(value_input_exception {
            value_input_exception::reason_code::value_out_of_range,
            string_builder {}
                    << "value out of range: " << value << ", "
                    << "must be in [" << std::numeric_limits<std::int32_t>::min() << ", "
                    << std::numeric_limits<std::int32_t>::max() << "]"
                    << string_builder::to_string,
    });
}

void throw_decimal_coefficient_out_of_range(std::size_t nbytes) {
    throw_exception(value_input_exception {
            value_input_exception::reason_code::value_out_of_range,
            string_builder {}
                    << "decimal value out of range: coefficient bytes=" << nbytes << ", "
                    << "must be <= " << (max_decimal_coefficient_size - 1) << ", "
                    << "or = " << max_decimal_coefficient_size  << " and the first byte is 0x00 or 0xff"
                    << string_builder::to_string,
    });
}

void throw_size_out_of_range(std::uint64_t size, std::uint64_t limit) {
    throw_exception(value_input_exception {
            value_input_exception::reason_code::value_out_of_range,
            string_builder {}
                    << "too large size: " << size << ", "
                    << "must be less than" << limit
                    << string_builder::to_string,
    });
}

} // namespace jogasaki::serializer
