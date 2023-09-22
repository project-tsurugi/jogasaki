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

#include <ostream>

#include <jogasaki/commit_response.h>

namespace jogasaki::api {

/**
 * @brief commit option
 * @details this is used to assign values to commit option
 */
class commit_option {
public:
    commit_option(commit_option const& other) = default;
    commit_option& operator=(commit_option const& other) = default;
    commit_option(commit_option&& other) noexcept = default;
    commit_option& operator=(commit_option&& other) noexcept = default;
    ~commit_option() = default;

    explicit commit_option(
        bool auto_dispose_on_success = false,
        commit_response_kind commit_response = commit_response_kind::undefined
    ) :
        auto_dispose_on_success_(auto_dispose_on_success),
        commit_response_(commit_response)
    {}

    commit_option& auto_dispose_on_success(bool arg) noexcept {
        auto_dispose_on_success_ = arg;
        return *this;
    }

    [[nodiscard]] bool auto_dispose_on_success() const noexcept {
        return auto_dispose_on_success_;
    }

    commit_option& commit_response(commit_response_kind arg) noexcept {
        commit_response_ = arg;
        return *this;
    }

    [[nodiscard]] commit_response_kind commit_response() const noexcept {
        return commit_response_;
    }

private:
    bool auto_dispose_on_success_ = false;
    commit_response_kind commit_response_ = commit_response_kind::undefined;
};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, commit_option const& value) {
    out << std::boolalpha;
    out << "auto_dispose_on_success:" << value.auto_dispose_on_success();
    out << " commit_response:" << value.commit_response();
    return out;
}
}
