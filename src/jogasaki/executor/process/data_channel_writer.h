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

#include <msgpack.hpp>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/record_writer.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/api/data_channel.h>

namespace jogasaki::executor::process {

using takatori::util::maybe_shared_ptr;

class cache_align data_channel_writer : public record_writer {
public:
    data_channel_writer(data_channel_writer const& other) = delete;
    data_channel_writer& operator=(data_channel_writer const& other) = delete;
    data_channel_writer(data_channel_writer&& other) noexcept = delete;
    data_channel_writer& operator=(data_channel_writer&& other) noexcept = delete;

    data_channel_writer(
        api::data_channel& channel,
        maybe_shared_ptr<meta::record_meta> meta
    );

    bool write(accessor::record_ref rec) override;

    void flush() override;

    void release() override;

private:
    api::data_channel* channel_{};
    std::shared_ptr<api::writer> writer_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    msgpack::sbuffer buf_{0};
};

}
