/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace tateyama::service_cli {

inline std::string_view trim(std::string_view s) {
    while(! s.empty() && std::isspace(static_cast<unsigned char>(s.front())) != 0) {
        s.remove_prefix(1);
    }
    while(! s.empty() && std::isspace(static_cast<unsigned char>(s.back())) != 0) {
        s.remove_suffix(1);
    }
    return s;
}

inline bool starts_with(std::string_view s, std::string_view prefix) {
    return s.substr(0, prefix.size()) == prefix;
}

inline std::string to_lower(std::string_view s) {
    std::string ret{s};
    std::transform(ret.begin(), ret.end(), ret.begin(), [](unsigned char c) { return std::tolower(c); });
    return ret;
}

inline std::string to_upper(std::string_view s) {
    std::string ret{s};
    std::transform(ret.begin(), ret.end(), ret.begin(), [](unsigned char c) { return std::toupper(c); });
    return ret;
}

/**
 * @brief returns the leading word composed of alphanumeric characters and underscore
 */
inline std::string_view first_word(std::string_view s) {
    std::size_t i = 0;
    while(i < s.size() && (std::isalnum(static_cast<unsigned char>(s[i])) != 0 || s[i] == '_')) {
        ++i;
    }
    return s.substr(0, i);
}

}  // namespace tateyama::service_cli
