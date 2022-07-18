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
#include "data_channel_writer.h"

#include <takatori/util/fail.h>
#include <jogasaki/common.h>
#include <jogasaki/utils/result_serialization.h>

namespace jogasaki::executor::io {

using takatori::util::fail;

bool data_channel_writer::write(accessor::record_ref rec) {
    if(! utils::write_msg(rec, buf_, meta_.get())) {
        return false;
    }
    {
        trace_scope_name("writer::write");  //NOLINT
        writer_->write(buf_.data(), buf_.size());
    }
    {
        trace_scope_name("writer::commit");  //NOLINT
        writer_->commit();
    }
    return true;
}

void data_channel_writer::flush() {
    if (writer_) {
        writer_->commit();
    }
}

void data_channel_writer::release() {
    {
        trace_scope_name("data_channel::release");  //NOLINT
        channel_->release(*writer_);
    }
    writer_ = nullptr;
}

data_channel_writer::data_channel_writer(
    api::data_channel& channel,
    std::shared_ptr<api::writer> writer,
    maybe_shared_ptr<meta::record_meta> meta
) :
    channel_(std::addressof(channel)),
    writer_(std::move(writer)),
    meta_(std::move(meta))
{}

}
