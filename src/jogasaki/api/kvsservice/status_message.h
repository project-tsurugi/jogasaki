/*
 * Copyright 2018-2023 tsurugi project.
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

#include <string>
#include <jogasaki/api/kvsservice/status.h>

namespace jogasaki::api::kvsservice {

class status_message {
public:
    explicit status_message(status code) : status_(code) {};
    status_message(status code, std::string_view message) : status_(code), message_(message){};
    [[nodiscard]] status status_code() const noexcept { return status_; };
    [[nodiscard]] std::string *message() noexcept { return message_.empty() ? nullptr : &message_; };

private:
    status status_{};
    std::string message_{};
};
}
