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

namespace jogasaki::executor::io {

using takatori::util::fail;

constexpr static std::size_t writer_work_buffer_size = 4096;

bool data_channel_writer::write(accessor::record_ref rec) {
    if(buf_.size() == 0) {
        buf_ = msgpack::sbuffer{writer_work_buffer_size}; // automatically expands when capacity is not sufficient
    }
    buf_.clear();
    for (std::size_t i=0, n=meta_->field_count(); i < n; ++i) {
        if (rec.is_null(meta_->nullity_offset(i))) {
            msgpack::pack(buf_, msgpack::type::nil_t());
        } else {
            using k = jogasaki::meta::field_type_kind;
            auto os = meta_->value_offset(i);
            switch (meta_->at(i).kind()) {
                case k::int4: msgpack::pack(buf_, rec.get_value<meta::field_type_traits<k::int4>::runtime_type>(os)); break;
                case k::int8: msgpack::pack(buf_, rec.get_value<meta::field_type_traits<k::int8>::runtime_type>(os)); break;
                case k::float4: msgpack::pack(buf_, rec.get_value<meta::field_type_traits<k::float4>::runtime_type>(os)); break;
                case k::float8: msgpack::pack(buf_, rec.get_value<meta::field_type_traits<k::float8>::runtime_type>(os)); break;
                case k::character: {
                    auto text = rec.get_value<meta::field_type_traits<k::character>::runtime_type>(os);
                    msgpack::pack(buf_, static_cast<std::string_view>(text));
                    break;
                }
                default:
                    fail();
            }
        }
    }
    {
        trace_scope_name("writer::write");  //NOLINT
        writer_->write(buf_.data(), buf_.size());
    }
    {
        trace_scope_name("writer::commit");  //NOLINT
        writer_->commit();
    }
    return false;
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
