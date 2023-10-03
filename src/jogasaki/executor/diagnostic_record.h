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

#include <type_traits>
#include <string_view>
#include <string>
#include <ostream>

namespace jogasaki::executor {

/**
 * @brief diagnostic information.
 * @tparam T the diagnostic code type
 */
template<class T>
class diagnostic_record {
public:
    /// @brief the diagnostic code type.
    using code_type = T;
    /// @brief the diagnostic message type.
    using message_type = std::string;

    /**
     * @brief creates a new empty instance.
     */
    diagnostic_record() noexcept = default;

    /**
     * @brief creates a new instance.
     * @param code the diagnostic code
     * @param message the optional diagnostic message
     */
    diagnostic_record(code_type code, message_type message) noexcept :
    code_(std::move(code)),
    message_(std::move(message))
    {}

    /**
     * @brief creates a new instance.
     * @param code the diagnostic code
     */
    explicit diagnostic_record(code_type code) noexcept :
        diagnostic_record(std::move(code), {})
        {}

    /**
     * @brief returns the diagnostic code.
     * @return the diagnostic code
     */
    [[nodiscard]] code_type code() const noexcept {
        return code_;
    }

    /**
     * @brief returns the diagnostic message.
     * @return the diagnostic message
     */
    [[nodiscard]] message_type const& message() const noexcept {
        return message_;
    }

private:
    code_type code_ {};
    message_type message_ {};
};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output stream
 */
template<class T>
inline std::ostream& operator<<(std::ostream& out, diagnostic_record<T> const& value) {
    return out << "diagnostic("
        << "code=" << value.code() << ", "
        << "message='" << value.message() << "')";
}

} // namespace jogasaki::executor
