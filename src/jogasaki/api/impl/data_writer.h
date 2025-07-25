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

#include <atomic>
#include <cstddef>
#include <memory>
#include <string_view>

#include <tateyama/api/server/writer.h>
#include <tateyama/status.h>

#include <jogasaki/api/writer.h>
#include <jogasaki/status.h>

namespace jogasaki::api::impl {

class data_writer : public api::writer {
public:
    data_writer() = default;

    explicit data_writer(std::shared_ptr<tateyama::api::server::writer> origin);

    status write(char const* data, std::size_t length) override;

    status commit() override;

    [[nodiscard]] std::shared_ptr<tateyama::api::server::writer> const& origin() const noexcept;

private:
    std::shared_ptr<tateyama::api::server::writer> origin_{};
};

}

