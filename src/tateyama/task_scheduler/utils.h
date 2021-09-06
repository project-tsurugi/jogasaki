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
#include <locale>
#include <ostream>
#include <sstream>

#include <takatori/util/detect.h>

namespace tateyama::task_scheduler {

namespace details {

template<class T>
using with_init_type = decltype( std::declval<T&>().init(static_cast<std::size_t>(0)) );

}

/**
 * @brief utility to check if given type has the init(std::size_t) member function
 * @tparam T
 */
template<class T>
inline constexpr bool has_init_v = takatori::util::is_detected_v<details::with_init_type, T>;

}


