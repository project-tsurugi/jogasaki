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

#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <chrono>
#include <memory>

namespace jogasaki::utils {

template<class T>
class delete_aligned {
public:
    void operator()(T* p) {
        std::free(p); //NOLINT
    }
};

template<class T> using aligned_array = std::unique_ptr<T[], delete_aligned<T>>;  //NOLINT

template<class T>
[[nodiscard]] aligned_array<T> make_aligned_array(size_t alignment, size_t size) {
    return aligned_array<T>( static_cast<T*>(std::aligned_alloc(alignment, size)), delete_aligned<T>{});
}

} // namespace
