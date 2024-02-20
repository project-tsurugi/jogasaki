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

#include <jogasaki/api/data_channel.h>
#include <jogasaki/executor/io/data_channel_writer.h>
#include <jogasaki/executor/io/null_record_channel_writer.h>
#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/meta/external_record_meta.h>

namespace jogasaki::executor::io {

/**
 * @brief adaptor to adapt api::data_channel to executor::record_channel
 */
class null_record_channel : public executor::io::record_channel {
public:
    /**
     * @brief create new object
     * @param channel the source channel object to adapt
     */
    null_record_channel() = default;

    /**
     * @brief acquire record writer
     * @param wrt [out] the acquired writer
     * @return status::ok when successful
     * @return any other error
     */
    status acquire(std::shared_ptr<record_writer>& wrt) override {
        wrt = writers_.emplace_back(std::make_shared<null_record_channel_writer>(meta_->origin()));
        return status::ok;
    }

    /**
     * @brief setter of the metadata
     * @param m metadata of the channel output
     * @return status::ok when successful
     * @return any other error
     */
    status meta(maybe_shared_ptr<meta::external_record_meta> m) override {
        meta_ = std::move(m);
        return status::ok;
    }

    /**
     * @brief accessor for channel stats
     */
    executor::io::record_channel_stats& statistics() override {
        return stats_;
    }
private:
    std::vector<std::shared_ptr<null_record_channel_writer>> writers_{};
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    executor::io::record_channel_stats stats_{};
};

}  // namespace jogasaki::executor::io
