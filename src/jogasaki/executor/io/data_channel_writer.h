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

#include <cstddef>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/api/writer.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/serializer/value_writer.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::io {

using takatori::util::maybe_shared_ptr;

class record_channel_adapter;

/**
 * @brief the writer writes output records into api::data_channel in result set encoding
 */
class cache_align data_channel_writer : public record_writer {
public:
    using value_writer = jogasaki::serializer::value_writer<api::writer, std::size_t>;

    /**
     * @brief create empty object
     */
    data_channel_writer() = default;

    data_channel_writer(data_channel_writer const& other) = delete;
    data_channel_writer& operator=(data_channel_writer const& other) = delete;
    data_channel_writer(data_channel_writer&& other) noexcept = delete;
    data_channel_writer& operator=(data_channel_writer&& other) noexcept = delete;

    /**
     * @brief create new object
     */
    data_channel_writer(
        record_channel_adapter& parent,
        std::shared_ptr<api::writer> writer,
        maybe_shared_ptr<meta::record_meta> meta
    );

    /**
     * @brief destruct object
     */
    ~data_channel_writer() override = default;

    /**
     * @brief write output record
     */
    bool write(accessor::record_ref rec) override;

    /**
     * @brief flush the buffer
     */
    void flush() override;

    /**
     * @brief release the object
     */
    void release() override;

private:
    record_channel_adapter* parent_{};
    std::shared_ptr<api::writer> writer_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::shared_ptr<value_writer> value_writer_{};
    std::size_t write_record_count_{};
};

}  // namespace jogasaki::executor::io
