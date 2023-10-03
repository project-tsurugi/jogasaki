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

#include <iostream>
#include <iomanip>
#include <limits>

namespace jogasaki::utils {

namespace details {

template<class T>
class hex {
public:
    static constexpr std::size_t default_width = (std::numeric_limits<T>::digits + 1) / 4;

    explicit hex(T const& value, std::size_t width = default_width) :
        value_(value), width_(width)
    {}

    void write(std::ostream& stream) const {
        auto flags = stream.setf(std::ios_base::hex, std::ios_base::basefield);
        char fill = stream.fill('0');
        stream << std::setw(width_) << value_;
        stream.fill(fill);
        stream.setf(flags, std::ios_base::basefield);
    }

private:
    T const& value_;
    std::size_t width_{};
};

template <typename T>
std::ostream& operator<<(std::ostream& stream, details::hex<T> const& value) {
    value.write(stream);
    return stream;
}

} // namespace details

template <typename T>
details::hex<T> hex(T const& value, std::size_t width = details::hex<T>::default_width) {
    return details::hex<T>(value, width);
}

}

