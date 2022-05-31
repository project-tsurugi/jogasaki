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
 * @brief adaptor to adapt data_channel to record_channel
 */
class record_channel_adapter : public executor::record_channel {
public:
    explicit record_channel_adapter(
        maybe_shared_ptr<api::data_channel> channel
    ) noexcept :
        channel_(std::move(channel))
    {}

    status acquire(std::shared_ptr<executor::record_writer>& wrt) override {
        std::shared_ptr<api::writer> writer;
        if(auto res = channel_->acquire(writer); res != status::ok) {
            return res;
        }
        wrt = std::make_shared<data_channel_writer>(*channel_, std::move(writer), meta_);
        return status::ok;
    }

    api::data_channel& channel() {
        return *channel_;
    }

    status meta(maybe_shared_ptr<meta::record_meta> m) override {
        meta_ = std::move(m);
        return status::ok;
    }
private:
    maybe_shared_ptr<api::data_channel> channel_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
};

}
