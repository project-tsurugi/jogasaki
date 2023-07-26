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
#include "parquet_writer.h"

#include <iomanip>
#include <array>

#include <glog/logging.h>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/writer.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/constants.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/utils/decimal.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;

using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using parquet::LogicalType;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

parquet_writer::parquet_writer(maybe_shared_ptr<meta::external_record_meta> meta) :
    meta_(std::move(meta))
{}

void parquet_writer::new_row_group() {
    if(row_group_writer_) {
        row_group_writer_->Close();
        column_writers_.clear();
    }
    row_group_writer_ = file_writer_->AppendBufferedRowGroup();
    column_writers_.reserve(meta_->field_count());
    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        column_writers_.emplace_back(row_group_writer_->column(i));
    }
}

bool parquet_writer::init(std::string_view path) {
    try {
        path_ = std::string{path};
        PARQUET_ASSIGN_OR_THROW(fs_ , ::arrow::io::FileOutputStream::Open(path_.string()));
        auto [schema, colopts] = create_schema();
        column_options_ = std::move(colopts);
        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::SNAPPY);
        file_writer_ = parquet::ParquetFileWriter::Open(fs_, schema, builder.build());
        new_row_group();
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Parquet writer init error: " << e.what();
        return false;
    }
    return true;
}

bool parquet_writer::write(accessor::record_ref ref) {
    try {
        using k = meta::field_type_kind;
        for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
            bool null = ref.is_null(meta_->nullity_offset(i)) && meta_->nullable(i);
            bool success{false};
            switch(meta_->at(i).kind()) {
                case k::int4: success = write_int4(i, ref.get_value<std::int32_t>(meta_->value_offset(i)), null); break;
                case k::int8: success = write_int8(i, ref.get_value<std::int64_t>(meta_->value_offset(i)), null); break;
                case k::float4: success = write_float4(i, ref.get_value<float>(meta_->value_offset(i)), null); break;
                case k::float8: success = write_float8(i, ref.get_value<double>(meta_->value_offset(i)), null); break;
                case k::character: success = write_character(i, ref.get_value<accessor::text>(meta_->value_offset(i)), null); break;
                case k::decimal: success = write_decimal(i, ref.get_value<runtime_t<meta::field_type_kind::decimal>>(meta_->value_offset(i)), null, column_options_[i]); break;
                case k::date: success = write_date(i, ref.get_value<runtime_t<meta::field_type_kind::date>>(meta_->value_offset(i)), null); break;
                case k::time_of_day: success = write_time_of_day(i, ref.get_value<runtime_t<meta::field_type_kind::time_of_day>>(meta_->value_offset(i)), null); break;
                case k::time_point: success = write_time_point(i, ref.get_value<runtime_t<meta::field_type_kind::time_point>>(meta_->value_offset(i)), null); break;
                default:
                    break;
            }
            if(! success) {
                return false;
            }
        }
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Parquet writer write error: " << e.what();
        return false;
    }
    ++write_count_;
    return true;
}

template <class T>
bool write_null(T* writer) {
    int16_t definition_level = 0;
    writer->WriteBatch(1, &definition_level, nullptr, nullptr);
    return true;
}

bool parquet_writer::write_int4(std::size_t colidx, int32_t v, bool null) {
    auto* writer = static_cast<parquet::Int32Writer*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        return write_null(writer);
    }
    int16_t definition_level = 1;
    writer->WriteBatch(1, &definition_level, nullptr, &v);
    return true;
}

bool parquet_writer::write_int8(std::size_t colidx, std::int64_t v, bool null) {
    auto* writer = static_cast<parquet::Int64Writer*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        return write_null(writer);
    }
    int16_t definition_level = 1;
    writer->WriteBatch(1, &definition_level, nullptr, &v);
    return true;
}

bool parquet_writer::write_float4(std::size_t colidx, float v, bool null) {
    auto* writer = static_cast<parquet::FloatWriter*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        return write_null(writer);
    }
    int16_t definition_level = 1;
    writer->WriteBatch(1, &definition_level, nullptr, &v);
    return true;
}

bool parquet_writer::write_float8(std::size_t colidx, double v, bool null) {
    auto* writer = static_cast<parquet::DoubleWriter*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        return write_null(writer);
    }
    int16_t definition_level = 1;
    writer->WriteBatch(1, &definition_level, nullptr, &v);
    return true;
}

bool parquet_writer::write_character(std::size_t colidx, accessor::text v, bool null) {
    auto* writer = static_cast<parquet::ByteArrayWriter*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        return write_null(writer);
    }

    parquet::ByteArray value{};
    auto sv = static_cast<std::string_view>(v);
    int16_t definition_level = 1;
    value.ptr = reinterpret_cast<const uint8_t*>(sv.data());  //NOLINT
    value.len = sv.size();
    writer->WriteBatch(1, &definition_level, nullptr, &value);
    return true;
}

bool parquet_writer::write_decimal(std::size_t colidx, runtime_t<meta::field_type_kind::decimal> v, bool null, details::column_option const& colopt) {
    auto* writer = static_cast<parquet::ByteArrayWriter*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        return write_null(writer);
    }
    auto sv = static_cast<decimal::Decimal>(v);
    decimal::context.clear_status();
    auto y = sv.rescale(-static_cast<std::int64_t>(colopt.scale_));
    if((decimal::context.status() & MPD_Inexact) != 0) {
        // value error
        VLOG_LP(log_error) << "value error: decimal rescaling failed. src=" << v << " scale=" << colopt.scale_;
        return false;
    }
    utils::decimal_buffer out{};
    auto [hi, lo, sz] = utils::make_signed_coefficient_full(takatori::decimal::triple{y});
    utils::create_decimal(v.sign(), lo, hi, sz, out);

    parquet::ByteArray value{};
    int16_t definition_level = 1;
    value.ptr = reinterpret_cast<const uint8_t*>(out.data());  //NOLINT
    value.len = sz;
    writer->WriteBatch(1, &definition_level, nullptr, &value);
    return true;
}

bool parquet_writer::write_date(std::size_t colidx, runtime_t<meta::field_type_kind::date> v, bool null) {
    auto d = static_cast<std::int32_t>(v.days_since_epoch());
    return write_int4(colidx, d, null);
}

bool parquet_writer::write_time_of_day(std::size_t colidx, runtime_t<meta::field_type_kind::time_of_day> v, bool null) {
    auto ns = static_cast<std::int64_t>(v.time_since_epoch().count());
    return write_int8(colidx, ns, null);
}

bool parquet_writer::write_time_point(std::size_t colidx, runtime_t<meta::field_type_kind::time_point> v, bool null) {
    auto secs = static_cast<std::int64_t>(v.seconds_since_epoch().count());
    auto subsecs = static_cast<std::int64_t>(v.subsecond().count());
    return write_int8(colidx, secs*1000*1000*1000 + subsecs, null);
}

bool parquet_writer::close() {
    try {
        file_writer_->Close();
        // Write the bytes to file
        DCHECK(fs_->Close().ok());
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Parquet writer close error: " << e.what();
        return false;
    }
    return true;
}

std::shared_ptr<parquet_writer>
parquet_writer::open(maybe_shared_ptr<meta::external_record_meta> meta, std::string_view path) {
    auto ret = std::make_shared<parquet_writer>(std::move(meta));
    if(ret->init(path)) {
        return ret;
    }
    return {};
}

std::pair<std::shared_ptr<parquet::schema::GroupNode>, std::vector<details::column_option>>
parquet_writer::create_schema() {
    parquet::schema::NodeVector fields{};
    fields.reserve(meta_->field_count());
    std::vector<details::column_option> options{};
    options.resize(meta_->field_count());
    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        std::string name{};
        if(auto o = meta_->field_name(i)) {
            name = o.value();
        }
        switch(meta_->at(i).kind()) {
            case meta::field_type_kind::int4: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Int(32, true), Type::INT32));
                break;
            }
            case meta::field_type_kind::int8: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Int(64, true), Type::INT64));
                break;
            }
            case meta::field_type_kind::float4: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, Type::FLOAT, ConvertedType::NONE));
                break;
            }
            case meta::field_type_kind::float8: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, Type::DOUBLE, ConvertedType::NONE));
                break;
            }
            case meta::field_type_kind::character: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::String(), Type::BYTE_ARRAY));
                break;
            }
            case meta::field_type_kind::decimal: {
                auto opt = meta_->at(i).option<meta::field_type_kind::decimal>();
                std::size_t p = opt->precision_.has_value() ? *opt->precision_ : decimal_default_precision;
                std::size_t s = opt->scale_.has_value() ? *opt->scale_: dumped_decimal_default_scale;
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Decimal(p, s), Type::BYTE_ARRAY));
                options[i].precision_ = p;
                options[i].scale_ = s;
                break;
            }
            case meta::field_type_kind::date: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Date(), Type::INT32));
                break;
            }
            case meta::field_type_kind::time_of_day: {
                auto opt = meta_->at(i).option<meta::field_type_kind::time_of_day>();
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Time(opt->with_offset_, parquet::LogicalType::TimeUnit::NANOS), Type::INT64));
                break;
            }
            case meta::field_type_kind::time_point: {
                auto opt = meta_->at(i).option<meta::field_type_kind::time_point>();
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Timestamp(opt->with_offset_, parquet::LogicalType::TimeUnit::NANOS), Type::INT64));
                break;
            }
            default:
                break;
        }
    }
    return {
        std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields)),
        std::move(options)
    };
}

std::string parquet_writer::path() const noexcept {
    return path_.string();
}

std::size_t parquet_writer::write_count() const noexcept {
    return write_count_;
}

}
