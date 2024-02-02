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

#include <memory>

#include <tateyama/api/server/request.h>

namespace jogasaki {

class request_info {
public:
    request_info() = default;

    explicit request_info(std::size_t id) :
        id_(id)
    {}

    request_info(std::size_t id, std::shared_ptr<tateyama::api::server::request const> src) :
        id_(id),
        request_source_(std::move(src))
    {}

    [[nodiscard]] std::size_t id() const noexcept {
        return id_;
    }

    [[nodiscard]] std::shared_ptr<tateyama::api::server::request const> const& request_source() const noexcept {
        return request_source_;
    }
private:
    std::size_t id_{};
    std::shared_ptr<tateyama::api::server::request const> request_source_{};
};

}  // namespace jogasaki
