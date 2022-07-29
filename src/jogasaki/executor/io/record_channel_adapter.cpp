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
#include "record_channel_adapter.h"

#include <takatori/util/fail.h>

#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/data_channel.h>

namespace jogasaki::executor::io {

using takatori::util::fail;

record_channel_adapter::record_channel_adapter(maybe_shared_ptr<api::data_channel> channel) noexcept:
    channel_(std::move(channel))
{}

status record_channel_adapter::acquire(std::shared_ptr<record_writer>& wrt) {
    std::shared_ptr<api::writer> writer;
    if(auto res = channel_->acquire(writer); res != status::ok) {
        return res;
    }
    wrt = std::make_shared<data_channel_writer>(*channel_, std::move(writer), meta_->origin());
    ++acquired_writers_;
    return status::ok;
}

api::data_channel& record_channel_adapter::channel() {
    return *channel_;
}

status record_channel_adapter::meta(maybe_shared_ptr<meta::external_record_meta> m) {
    meta_ = std::move(m);
    return status::ok;
}

status record_channel_adapter::close() {
    if (acquired_writers_ == 0) {
        // result set requires end-of-contents marker even if there is no output records
        // so acquire and release here in order to emit the marker by release
        std::shared_ptr<record_writer> writer{};
        if(auto res = acquire(writer); res != status::ok) {
            fail();
        }
        writer->release();
    }
    return status::ok;
}
}
