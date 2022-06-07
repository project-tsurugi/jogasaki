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

#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/io/data_channel_writer.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::executor::io {

struct dump_cfg {
    constexpr static std::size_t undefined = static_cast<std::size_t>(-1);

    std::size_t max_records_per_file_{undefined};
    std::size_t max_file_byte_size_{undefined};
};

/**
 * @brief record channel to execute dump
 */
class dump_channel : public record_channel {
public:
    /**
     * @brief create new object
     * @param channel the output channel to send out dump file names
     * @param directory the directory path to dump the data into
     */
    explicit dump_channel(
        maybe_shared_ptr<record_channel> channel,
        std::string_view directory,
        dump_cfg cfg = {}
    ) noexcept;

    /**
     * @brief acquire record writer
     * @param wrt [out] the acquired writer
     * @return status::ok when successful
     * @return any other error
     */
    status acquire(std::shared_ptr<record_writer>& wrt) override;

    /**
     * @brief accessor to filename output channel object
     * @return the output channel
     */
    record_channel& channel();

    /**
     * @brief setter of the metadata
     * @param m metadata of the channel output
     * @return status::ok when successful
     * @return any other error
     */
    status meta(maybe_shared_ptr<meta::external_record_meta> m) override;

    /**
     * @brief accessor to dump directory
     * @return the directory path
     */
    [[nodiscard]] std::string_view directory() const noexcept;

    /**
     * @brief accessor to query metadata
     * @return the metadata of the executed query output
     */
    [[nodiscard]] maybe_shared_ptr<meta::external_record_meta> const& meta() const noexcept;

    /**
     * @brief accessor to file name output record metadata
     * @return the metadata used for the filename output
     */
    [[nodiscard]] maybe_shared_ptr<meta::external_record_meta> const& file_name_record_meta() const noexcept;

    /**
     * @brief accessor to dump file name prefix
     * @return the prefix
     */
    [[nodiscard]] std::string_view prefix() const noexcept;

private:
    maybe_shared_ptr<record_channel> channel_{};
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    maybe_shared_ptr<meta::external_record_meta> file_name_record_meta_{};
    std::string directory_{};
    std::string prefix_{};
    dump_cfg cfg_{};
    std::atomic_size_t writer_id_src_{0};
};

}
