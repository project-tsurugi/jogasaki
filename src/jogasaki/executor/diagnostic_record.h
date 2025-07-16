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

#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace jogasaki::executor {

class diagnostic_argument {
public:
    diagnostic_argument() = default;

    std::stringstream& entity() {
        return entity_;
    }

    std::string str() const {
        return entity_.str();
    }

private:
    std::stringstream entity_{};
};

template <class T>
inline diagnostic_argument& operator<<(diagnostic_argument& out, T&& value) {
    out.entity() << value;
    return out;
}

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
    /// @brief the argument list type.
    using arguments_type = std::vector<diagnostic_argument>;

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

    /**
     * @brief create new argument and returns the reference
     * @return the reference to the argument, which is available until next call of this method
     */
    [[nodiscard]] diagnostic_argument& new_argument() noexcept {
        auto& a = arguments_.emplace_back();
        return a;
    }

    /**
     * @brief accessor for arguments
     * @return the list of arguments
     */
    [[nodiscard]] arguments_type const& arguments() const noexcept {
        return arguments_;
    }
private:
    code_type code_{};
    message_type message_{};
    arguments_type arguments_{};
};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output stream
 */
template<class T>
inline std::ostream& operator<<(std::ostream& out, diagnostic_record<T> const& value) {
    out << "diagnostic("
        << "code=" << value.code() << ", "
        << "message='" << value.message() << "'";
        if(! value.arguments().empty()) {
            out << ", args=[";
            bool first = true;
            for(auto&& e : value.arguments()) {
                if(! first) {
                    out << ", ";
                }
                out << "'" << e.str() << "'";
                first = false;
            }
            out << "]";
        }
        out << ")";
    return out;
}

} // namespace jogasaki::executor
