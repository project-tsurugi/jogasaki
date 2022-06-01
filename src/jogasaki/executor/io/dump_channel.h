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
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::executor {

/**
 * @brief adaptor to adapt api::data_channel to executor::record_channel
 */
class dump_channel : public executor::record_channel {
public:
    /**
     * @brief create new object
     * @param channel the source channel object to adapt
     */
    explicit dump_channel(
        maybe_shared_ptr<executor::record_channel> channel,
        std::string_view directory
    ) noexcept;

    /**
     * @brief acquire record writer
     * @param wrt [out] the acquired writer
     * @return status::ok when successful
     * @return any other error
     */
    status acquire(std::shared_ptr<executor::record_writer>& wrt) override;

    /**
     * @brief accessor to original channel object
     * @return the source channel
     */
    executor::record_channel& channel();

    /**
     * @brief setter of the metadata
     * @param m metadata of the channel output
     * @return status::ok when successful
     * @return any other error
     */
    status meta(maybe_shared_ptr<meta::record_meta> m) override;

    [[nodiscard]] std::string_view directory() const noexcept;

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& file_name_record_meta() const noexcept {
        return file_name_record_meta_;
    }
private:
    maybe_shared_ptr<executor::record_channel> channel_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    maybe_shared_ptr<meta::record_meta> file_name_record_meta_{};
    std::string directory_{};
};

}
