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
#include "data_channel_writer.h"

#include <memory>
#include <ostream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <glog/logging.h>

#include <tateyama/common.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/executor/io/record_channel_stats.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/serializer/value_writer.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/trace_log.h>

namespace jogasaki::executor::io {

//NOLINTBEGIN
#define check_writer_rc(arg) \
    if(auto return_value = (arg); return_value != status::ok) { \
        log_exit; \
        return false; \
    }
//NOLINTEND

bool data_channel_writer::write(accessor::record_ref rec) {
    log_entry << "record_size:" << rec.size();
    auto n = meta_->field_count();
    check_writer_rc(value_writer_->write_row_begin(n));
    for (std::size_t i=0; i < n; ++i) {
        if (rec.is_null(meta_->nullity_offset(i))) {
            check_writer_rc(value_writer_->write_null());
        } else {
            using k = jogasaki::meta::field_type_kind;
            auto os = meta_->value_offset(i);
            auto& type = meta_->at(i);
            switch (type.kind()) {
                case k::boolean: check_writer_rc(value_writer_->write_int(rec.get_value<runtime_t<k::boolean>>(os))); break;
                case k::int4: check_writer_rc(value_writer_->write_int(rec.get_value<runtime_t<k::int4>>(os))); break;
                case k::int8: check_writer_rc(value_writer_->write_int(rec.get_value<runtime_t<k::int8>>(os))); break;
                case k::float4: check_writer_rc(value_writer_->write_float4(rec.get_value<runtime_t<k::float4>>(os))); break;
                case k::float8: check_writer_rc(value_writer_->write_float8(rec.get_value<runtime_t<k::float8>>(os))); break;
                case k::character: {
                    auto text = rec.get_value<runtime_t<k::character>>(os);
                    check_writer_rc(value_writer_->write_character(static_cast<std::string_view>(text)));
                    break;
                }
                case k::octet: {
                    auto binary = rec.get_value<runtime_t<k::octet>>(os);
                    check_writer_rc(value_writer_->write_octet(static_cast<std::string_view>(binary)));
                    break;
                }
                case k::decimal: check_writer_rc(value_writer_->write_decimal(rec.get_value<runtime_t<k::decimal>>(os))); break;
                case k::date: check_writer_rc(value_writer_->write_date(rec.get_value<runtime_t<k::date>>(os))); break;
                case k::time_of_day: {
                    if(type.option_unsafe<k::time_of_day>()->with_offset_) {
                        auto tm = rec.get_value<runtime_t<k::time_of_day>>(os);
                        auto offset_min = global::config_pool()->zone_offset();
                        tm += std::chrono::minutes(offset_min);
                        check_writer_rc(value_writer_->write_time_of_day_with_offset(tm, offset_min));
                        break;
                    }
                    check_writer_rc(value_writer_->write_time_of_day(rec.get_value<runtime_t<k::time_of_day>>(os)));
                    break;
                }
                case k::time_point: {
                    if(type.option_unsafe<k::time_point>()->with_offset_) {
                        auto tp = rec.get_value<runtime_t<k::time_point>>(os);
                        auto offset_min = global::config_pool()->zone_offset();
                        tp += std::chrono::minutes(offset_min);
                        check_writer_rc(value_writer_->write_time_point_with_offset(tp, offset_min));
                        break;
                    }
                    check_writer_rc(value_writer_->write_time_point(rec.get_value<runtime_t<k::time_point>>(os)));
                    break;
                }
                case k::blob: {
                    auto lob = rec.get_value<runtime_t<k::blob>>(os);
                    check_writer_rc(value_writer_->write_blob(static_cast<std::uint64_t>(lob.provider()), lob.object_id()));
                    break;
                }
                case k::clob: {
                    auto lob = rec.get_value<runtime_t<k::clob>>(os);
                    check_writer_rc(value_writer_->write_blob(static_cast<std::uint64_t>(lob.provider()), lob.object_id()));
                    break;
                }
                default:
                    fail_with_exception();
            }
        }
    }
    ++write_record_count_;
    {
        trace_scope_name("writer::commit");  //NOLINT
        writer_->commit();
    }
    log_exit;
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
        parent_->channel().release(*writer_);
    }
    writer_ = nullptr;
    value_writer_.reset();
    parent_->statistics().add_total_record(write_record_count_);
    write_record_count_ = 0;
}

data_channel_writer::data_channel_writer(
    record_channel_adapter& parent,
    std::shared_ptr<api::writer> writer,
    maybe_shared_ptr<meta::record_meta> meta
) :
    parent_(std::addressof(parent)),
    writer_(std::move(writer)),
    meta_(std::move(meta)),
    value_writer_(std::make_shared<value_writer>(*writer_))
{}

}  // namespace jogasaki::executor::io
