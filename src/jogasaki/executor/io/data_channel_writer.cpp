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

#include <memory>
#include <glog/logging.h>

#include <takatori/util/fail.h>

#include <jogasaki/common.h>
#include <jogasaki/utils/trace_log.h>

namespace jogasaki::executor::io {

using takatori::util::fail;

bool data_channel_writer::write(accessor::record_ref rec) {
    log_entry << "data_channel_writer::write() record_size:" << rec.size();
    auto n = meta_->field_count();
    value_writer_->write_row_begin(n);
    for (std::size_t i=0; i < n; ++i) {
        if (rec.is_null(meta_->nullity_offset(i))) {
            value_writer_->write_null();
        } else {
            using k = jogasaki::meta::field_type_kind;
            auto os = meta_->value_offset(i);
            auto& type = meta_->at(i);
            switch (type.kind()) {
                case k::int4: value_writer_->write_int(rec.get_value<runtime_t<k::int4>>(os)); break;
                case k::int8: value_writer_->write_int(rec.get_value<runtime_t<k::int8>>(os)); break;
                case k::float4: value_writer_->write_float4(rec.get_value<runtime_t<k::float4>>(os)); break;
                case k::float8: value_writer_->write_float8(rec.get_value<runtime_t<k::float8>>(os)); break;
                case k::character: {
                    auto text = rec.get_value<runtime_t<k::character>>(os);
                    value_writer_->write_character(static_cast<std::string_view>(text));
                    break;
                }
                case k::octet: {
                    auto binary = rec.get_value<runtime_t<k::octet>>(os);
                    value_writer_->write_octet(static_cast<std::string_view>(binary));
                    break;
                }
                case k::decimal: value_writer_->write_decimal(rec.get_value<runtime_t<k::decimal>>(os)); break;
                case k::date: value_writer_->write_date(rec.get_value<runtime_t<k::date>>(os)); break;
                case k::time_of_day: {
                    if(type.option_unsafe<k::time_of_day>()->with_offset_) {
                        // TODO timezone offset is zero for now
                        value_writer_->write_time_of_day_with_offset(rec.get_value<runtime_t<k::time_of_day>>(os), 0);
                        break;
                    }
                    value_writer_->write_time_of_day(rec.get_value<runtime_t<k::time_of_day>>(os));
                    break;
                }
                case k::time_point: {
                    if(type.option_unsafe<k::time_point>()->with_offset_) {
                        // TODO timezone offset is zero for now
                        value_writer_->write_time_point_with_offset(rec.get_value<runtime_t<k::time_point>>(os), 0);
                        break;
                    }
                    value_writer_->write_time_point(rec.get_value<runtime_t<k::time_point>>(os));
                    break;
                }
                default:
                    fail();
            }
        }
    }
    {
        trace_scope_name("writer::commit");  //NOLINT
        writer_->commit();
    }
    log_exit << "data_channel_writer::write()";
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
    value_writer_.reset();
}

data_channel_writer::data_channel_writer(
    api::data_channel& channel,
    std::shared_ptr<api::writer> writer,
    maybe_shared_ptr<meta::record_meta> meta
) :
    channel_(std::addressof(channel)),
    writer_(std::move(writer)),
    meta_(std::move(meta)),
    value_writer_(std::make_shared<value_writer>(*writer_))
{}

}
