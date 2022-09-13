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

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/logging.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/utils/decimal.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;

using parquet::ConvertedType;
using parquet::Repetition;
using parquet::Type;
using parquet::LogicalType;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

parquet_writer::parquet_writer(maybe_shared_ptr<meta::external_record_meta> meta) :
    meta_(std::move(meta))
{}

bool parquet_writer::init(std::string_view path) {
    try {
        path_ = std::string{path};
        PARQUET_ASSIGN_OR_THROW(fs_ , ::arrow::io::FileOutputStream::Open(path_.string()));
        auto schema = create_schema();
        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::SNAPPY);
        file_writer_ = parquet::ParquetFileWriter::Open(fs_, schema, builder.build());
        auto* rgwriter = file_writer_->AppendBufferedRowGroup();
        column_writers_.reserve(meta_->field_count());
        for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
            column_writers_.emplace_back(rgwriter->column(i));
        }
    } catch (std::exception const& e) {
        VLOG(log_error) << "Parquet writer init error: " << e.what();
        return false;
    }
    return true;
}

bool parquet_writer::write(accessor::record_ref ref) {
    try {
        using k = meta::field_type_kind;
        for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
            bool null = ref.is_null(meta_->nullity_offset(i)) && meta_->nullable(i);
            switch(meta_->at(i).kind()) {
                case k::int4: write_int4(i, ref.get_value<std::int32_t>(meta_->value_offset(i)), null); break;
                case k::int8: write_int8(i, ref.get_value<std::int64_t>(meta_->value_offset(i)), null); break;
                case k::float4: write_float4(i, ref.get_value<float>(meta_->value_offset(i)), null); break;
                case k::float8: write_float8(i, ref.get_value<double>(meta_->value_offset(i)), null); break;
                case k::character: write_character(i, ref.get_value<accessor::text>(meta_->value_offset(i)), null); break;
                case k::decimal: write_decimal(i, ref.get_value<runtime_t<meta::field_type_kind::decimal>>(meta_->value_offset(i)), null); break;
                case k::date: write_date(i, ref.get_value<runtime_t<meta::field_type_kind::date>>(meta_->value_offset(i)), null); break;
                case k::time_of_day: write_time_of_day(i, ref.get_value<runtime_t<meta::field_type_kind::time_of_day>>(meta_->value_offset(i)), null); break;
                case k::time_point: write_time_point(i, ref.get_value<runtime_t<meta::field_type_kind::time_point>>(meta_->value_offset(i)), null); break;
                default:
                    break;
            }
        }
    } catch (std::exception const& e) {
        VLOG(log_error) << "Parquet writer write error: " << e.what();
        return false;
    }
    ++write_count_;
    return true;
}

void parquet_writer::write_int4(std::size_t colidx, int32_t v, bool null) {
    auto* writer = static_cast<parquet::Int32Writer*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        int16_t definition_level = 0;
        writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }
    int16_t definition_level = 1;
    writer->WriteBatch(1, &definition_level, nullptr, &v);
}

void parquet_writer::write_int8(std::size_t colidx, std::int64_t v, bool null) {
    auto* writer = static_cast<parquet::Int64Writer*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        int16_t definition_level = 0;
        writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }
    int16_t definition_level = 1;
    writer->WriteBatch(1, &definition_level, nullptr, &v);
}

void parquet_writer::write_float4(std::size_t colidx, float v, bool null) {
    auto* writer = static_cast<parquet::FloatWriter*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        int16_t definition_level = 0;
        writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }
    int16_t definition_level = 1;
    writer->WriteBatch(1, &definition_level, nullptr, &v);
}

void parquet_writer::write_float8(std::size_t colidx, double v, bool null) {
    auto* writer = static_cast<parquet::DoubleWriter*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        int16_t definition_level = 0;
        writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }
    int16_t definition_level = 1;
    writer->WriteBatch(1, &definition_level, nullptr, &v);
}

void parquet_writer::write_character(std::size_t colidx, accessor::text v, bool null) {
    auto* writer = static_cast<parquet::ByteArrayWriter*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        int16_t definition_level = 0;
        writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }

    parquet::ByteArray value{};
    auto sv = static_cast<std::string_view>(v);
    int16_t definition_level = 1;
    value.ptr = reinterpret_cast<const uint8_t*>(sv.data());  //NOLINT
    value.len = sv.size();
    writer->WriteBatch(1, &definition_level, nullptr, &value);
}

constexpr std::size_t max_decimal_length = sizeof(std::uint64_t) * 2 + 1;

void create_decimal(
    std::int8_t sign,
    std::uint64_t lo,
    std::uint64_t hi,
    std::size_t sz,
    std::array<std::uint8_t, max_decimal_length>& out
    ) {
    auto base_ = out.data();
    std::size_t pos_ = 0;

    if (sz > sizeof(std::uint64_t) * 2) {
        // write sign bit
        *(base_ + pos_) = sign >= 0 ? '\x00' : '\xFF';  // NOLINT
        ++pos_;
        --sz;
    }

    for (std::size_t offset = 0, n = std::min(sz, sizeof(std::uint64_t)); offset < n; ++offset) {
        *(base_ + pos_ + sz - offset - 1) = static_cast<char>(lo >> (offset * 8U));;  //NOLINT
    }
    if (sz > sizeof(std::uint64_t)) {
        for (std::size_t offset = 0, n = std::min(sz - sizeof(std::uint64_t), sizeof(std::uint64_t)); offset < n; ++offset) {
            *(base_ + pos_ + sz - offset - sizeof(std::uint64_t) - 1) = static_cast<char>(hi >> (offset * 8U));;
        }
    }
}

void parquet_writer::write_decimal(std::size_t colidx, runtime_t<meta::field_type_kind::decimal> v, bool null) {
    auto* writer = static_cast<parquet::ByteArrayWriter*>(column_writers_[colidx]);  //NOLINT
    if (null) {
        int16_t definition_level = 0;
        writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }

    parquet::ByteArray value{};
    auto sv = static_cast<decimal::Decimal>(v);
    std::array<std::uint8_t, max_decimal_length> out{};
    auto [hi, lo, sz] = utils::make_signed_coefficient_full(v);
    create_decimal(v.sign(), lo, hi, sz, out);

    int16_t definition_level = 1;
    value.ptr = reinterpret_cast<const uint8_t*>(out.data());  //NOLINT
    value.len = sz;
    writer->WriteBatch(1, &definition_level, nullptr, &value);
}

void parquet_writer::write_date(std::size_t colidx, runtime_t<meta::field_type_kind::date> v, bool null) {
    auto d = static_cast<std::int32_t>(v.days_since_epoch());
    write_int4(colidx, d, null);
}

void parquet_writer::write_time_of_day(std::size_t colidx, runtime_t<meta::field_type_kind::time_of_day> v, bool null) {
    auto ns = static_cast<std::int64_t>(v.time_since_epoch().count());
    write_int8(colidx, ns, null);
}

void parquet_writer::write_time_point(std::size_t colidx, runtime_t<meta::field_type_kind::time_point> v, bool null) {
    auto secs = static_cast<std::int64_t>(v.seconds_since_epoch().count());
    auto subsecs = static_cast<std::int64_t>(v.subsecond().count());
    write_int8(colidx, secs*1000*1000*1000 + subsecs, null);
}

bool parquet_writer::close() {
    try {
        file_writer_->Close();
        // Write the bytes to file
        DCHECK(fs_->Close().ok());
    } catch (std::exception const& e) {
        VLOG(log_error) << "Parquet writer close error: " << e.what();
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

std::shared_ptr<GroupNode> parquet_writer::create_schema() {
    parquet::schema::NodeVector fields{};
    fields.reserve(meta_->field_count());
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
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Decimal(*opt->precision_, opt->scale_), Type::BYTE_ARRAY)); //TODO
                break;
            }
            case meta::field_type_kind::date: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Date(), Type::INT32));
                break;
            }
            case meta::field_type_kind::time_of_day: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Time(true, parquet::LogicalType::TimeUnit::NANOS), Type::INT64));
                break;
            }
            case meta::field_type_kind::time_point: {
                fields.push_back(PrimitiveNode::Make(name, Repetition::OPTIONAL, LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::NANOS), Type::INT64));
                break;
            }
            default:
                break;
        }
    }
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

std::string parquet_writer::path() const noexcept {
    return path_.string();
}

std::size_t parquet_writer::write_count() const noexcept {
    return write_count_;
}

}
