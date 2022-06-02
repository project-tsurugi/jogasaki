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
#include <jogasaki/utils/parquet_writer.h>
#include <jogasaki/executor/io/dump_channel.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

class dump_channel;

/**
 * @brief writer to execute dump
 * @details this writer save the query output into files and write down those files names to downstream writer
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
     * @param parent the owner channel of this object
     * @param writer output writer to send file name record
     * @param writer_index the unique 0-origin index of this writer among parent channel
     */
    dump_channel_writer(
        dump_channel& parent,
        maybe_shared_ptr<executor::record_writer> writer,
        std::size_t writer_index,
        dump_cfg cfg = {}
    );

    /**
     * @brief destruct object
     */
    ~dump_channel_writer() override = default;

    /**
     * @brief write query output record
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
    dump_channel* parent_{};
    maybe_shared_ptr<executor::record_writer> writer_{};
    std::shared_ptr<utils::parquet_writer> parquet_writer_{};
    std::size_t writer_index_{};
    std::size_t current_sequence_number_{};
    dump_cfg cfg_{};

    [[nodiscard]] std::string create_file_name(std::string_view prefix) const;
    void write_file_path(std::string_view path);
    void close_parquet_writer();
};

}
