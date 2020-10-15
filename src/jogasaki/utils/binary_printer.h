/*
 * Copyright 2018-2020 tsurugi project.
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

#include <iomanip>

#include <takatori/util/fail.h>

namespace jogasaki::utils {

using takatori::util::fail;

/**
 * @brief debug support to print binary value array
 */
class binary_printer {
public:
    constexpr binary_printer(void* ptr, std::size_t size) noexcept : ptr_(ptr), size_(size) {}

    friend std::ostream& operator<<(std::ostream& out, binary_printer const& value) {
        std::ios init(NULL);
        init.copyfmt(out);
        for(std::size_t idx = 0; idx < value.size_; ++idx) {
            if (idx != 0) {
                out << std::string_view("-");
            }
            out << std::hex << std::setw(2) << std::setfill('0')
                << static_cast<std::uint32_t>(*(static_cast<std::uint8_t*>(value.ptr_)+idx)); //NOLINT
        }
        out.copyfmt(init);
        return out;
    }

private:
    void* ptr_{};
    std::size_t size_{};
};

}
