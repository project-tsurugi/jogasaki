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

#include <memory>
#include <string_view>

#include <tateyama/api/server/data_channel.h>
#include <tateyama/status.h>

#include <jogasaki/api/data_channel.h>
#include <jogasaki/api/writer.h>
#include <jogasaki/status.h>

namespace jogasaki::api::impl {

class data_channel : public api::data_channel {
public:
    data_channel() = default;

    data_channel(
        std::shared_ptr<tateyama::api::server::data_channel> origin,
        std::size_t max_writers
    );

    status acquire(std::shared_ptr<writer>& wrt) override;

    status release(writer& wrt) override;

    [[nodiscard]] std::shared_ptr<tateyama::api::server::data_channel> const& origin() const noexcept;

    [[nodiscard]] std::optional<std::size_t> max_writer_count() override;

private:
    std::shared_ptr<tateyama::api::server::data_channel> origin_{};
    std::size_t max_writer_count_{};
};

}

