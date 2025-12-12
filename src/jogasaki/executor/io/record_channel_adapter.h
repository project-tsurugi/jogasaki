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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/data_channel.h>
#include <jogasaki/executor/io/data_channel_writer.h>
#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/executor/io/record_channel_stats.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/status.h>

namespace jogasaki::executor::io {

/**
 * @brief channel option for record_channel_adapter
 */
struct channel_option {
    /**
     * @brief transaction id (surrogate id)
     */
    std::uint64_t transaction_id_{};
};

/**
 * @brief adaptor to adapt api::data_channel to executor::record_channel
 */
class record_channel_adapter : public executor::io::record_channel {
public:
    /**
     * @brief create new object
     * @param channel the source channel object to adapt
     */
    explicit record_channel_adapter(maybe_shared_ptr<api::data_channel> channel) noexcept;

    /**
     * @brief set channel option
     * @param opt the channel option
     */
    void option(channel_option opt) noexcept;

    /**
     * @brief get channel option
     * @return the channel option
     */
    [[nodiscard]] channel_option const& option() const noexcept;

    /**
     * @brief acquire record writer
     * @param wrt [out] the acquired writer
     * @return status::ok when successful
     * @return any other error
     */
    status acquire(std::shared_ptr<record_writer>& wrt) override;

    /**
     * @brief accessor to original channel object
     * @return the source channel
     */
    api::data_channel& channel();

    /**
     * @brief setter of the metadata
     * @param m metadata of the channel output
     * @return status::ok when successful
     * @return any other error
     */
    status meta(maybe_shared_ptr<meta::external_record_meta> m) override;

    /**
     * @brief accessor for channel stats
     */
    record_channel_stats& statistics() override;

    /**
     * @brief accessor for record channel kind
     */
    [[nodiscard]] record_channel_kind kind() const noexcept override{
        return record_channel_kind::record_channel_adapter;
    }

    /**
     * @brief accessor for the maximum number of writers available on this channel
     * @return the max number of writers
     * @return std::nullopt if there is no maximum limit
     */
    [[nodiscard]] std::optional<std::size_t> max_writer_count() override;

private:
    maybe_shared_ptr<api::data_channel> channel_{};
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    record_channel_stats stats_{};
    channel_option option_{};
};

}  // namespace jogasaki::executor::io
