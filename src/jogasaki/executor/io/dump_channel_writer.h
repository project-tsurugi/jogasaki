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

#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/api/data_channel.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

class dump_channel;

/**
 * @brief the writer writes output records into api::data_channel in msgpack encoding
 */
class cache_align dump_channel_writer : public record_writer {
public:
    /**
     * @brief create empty object
     */
    dump_channel_writer() = default;

    dump_channel_writer(dump_channel_writer const& other) = delete;
    dump_channel_writer& operator=(dump_channel_writer const& other) = delete;
    dump_channel_writer(dump_channel_writer&& other) noexcept = delete;
    dump_channel_writer& operator=(dump_channel_writer&& other) noexcept = delete;

    /**
     * @brief create new object
     */
    dump_channel_writer(
        dump_channel& parent,
        maybe_shared_ptr<executor::record_writer> writer
    );

    /**
     * @brief destruct object
     */
    ~dump_channel_writer() override = default;

    /**
     * @brief write output record
     */
    bool write(accessor::record_ref rec) override;

    /**
     * @brief flush the buffer
     */
    void flush() override {

    }

    /**
     * @brief release the object
     */
    void release() override;

private:
    dump_channel* parent_{};
    maybe_shared_ptr<executor::record_writer> writer_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
};

}