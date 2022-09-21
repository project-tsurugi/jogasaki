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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/serializer/value_writer.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/api/data_channel.h>

namespace jogasaki::executor::io {

using takatori::util::maybe_shared_ptr;

class null_record_channel;

/**
 * @brief the writer writes output records into api::data_channel in result set encoding
 */
class cache_align null_record_channel_writer : public record_writer {
public:
    /**
     * @brief create empty object
     */
    null_record_channel_writer() = default;

    null_record_channel_writer(null_record_channel_writer const& other) = delete;
    null_record_channel_writer& operator=(null_record_channel_writer const& other) = delete;
    null_record_channel_writer(null_record_channel_writer&& other) noexcept = delete;
    null_record_channel_writer& operator=(null_record_channel_writer&& other) noexcept = delete;

    /**
     * @brief create new object
     */
    explicit null_record_channel_writer(
        maybe_shared_ptr<meta::record_meta> meta
    ) noexcept :
        meta_(std::move(meta))
    {}

    /**
     * @brief destruct object
     */
    ~null_record_channel_writer() override = default;

    /**
     * @brief write output record
     */
    bool write(accessor::record_ref) override {
        return true;
    }

    /**
     * @brief flush the buffer
     */
    void flush() override {
        // no-op
    }

    /**
     * @brief release the object
     */
    void release() override {
        // no-op : should be destructed together with parent null_record_channel
    }

private:
    maybe_shared_ptr<meta::record_meta> meta_{};
};

}
