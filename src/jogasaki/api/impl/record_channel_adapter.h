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

#include <jogasaki/executor/record_channel.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::api::impl {

class record_channel_adapter;

class record_channel_adapter_writer : public executor::record_writer {
public:
    record_channel_adapter_writer(
        record_channel_adapter& parent,
        std::shared_ptr<writer> writer
    ) noexcept :
        parent_(std::addressof(parent)),
        writer_(std::move(writer))
    {}

    bool write(accessor::record_ref rec) override {
        writer_->write(static_cast<char*>(rec.data()), rec.size());
        return false; //TODO
    }

    void flush() override {
        //no-op //TODO
    }

    void release() override;

private:
    record_channel_adapter* parent_{};
    std::shared_ptr<writer> writer_{};
};

class record_channel_adapter : public executor::record_channel {
public:
    explicit record_channel_adapter(
        maybe_shared_ptr<api::data_channel> channel
    ) noexcept :
        channel_(std::move(channel))
    {}

    status acquire(std::shared_ptr<executor::record_writer>& wrt) override {
        std::shared_ptr<writer> writer;
        if(auto res = channel_->acquire(writer); res != status::ok) {
            return res;
        }
        wrt = std::make_shared<record_channel_adapter_writer>(*this, std::move(writer));
        return status::ok;
    }

    api::data_channel& channel() {
        return *channel_;
    }
private:
    maybe_shared_ptr<api::data_channel> channel_{};
};

}
