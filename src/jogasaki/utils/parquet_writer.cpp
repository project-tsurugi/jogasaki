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

#include <glog/logging.h>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/logging.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::utils {

using takatori::util::fail;
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

bool parquet_writer::init(std::string_view path) {
    try {
        PARQUET_ASSIGN_OR_THROW(fs_ , ::arrow::io::FileOutputStream::Open(std::string{path}));
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
        VLOG(log_error) << "Parquet write error: " << e.what();
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
                default:
                    break;
            }
        }
    } catch (std::exception const& e) {
        VLOG(log_error) << "Parquet write error: " << e.what();
        return false;
    }
    return true;
}

void parquet_writer::write_int4(std::size_t colidx, int32_t v, bool null) {
    parquet::Int32Writer* int4_writer = static_cast<parquet::Int32Writer*>(column_writers_[colidx]);
    if (null) {
        int16_t definition_level = 0;
        int4_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }
    int16_t definition_level = 1;
    int4_writer->WriteBatch(1, &definition_level, nullptr, &v);
}

void parquet_writer::write_int8(std::size_t colidx, std::int64_t v, bool null) {
    parquet::Int64Writer* int8_writer = static_cast<parquet::Int64Writer*>(column_writers_[colidx]);
    if (null) {
        int16_t definition_level = 0;
        int8_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }
    int16_t definition_level = 1;
    int8_writer->WriteBatch(1, &definition_level, nullptr, &v);
}

void parquet_writer::write_float4(std::size_t colidx, float v, bool null) {
    parquet::FloatWriter* float_writer = static_cast<parquet::FloatWriter*>(column_writers_[colidx]);
    if (null) {
        int16_t definition_level = 0;
        float_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }
    int16_t definition_level = 1;
    float_writer->WriteBatch(1, &definition_level, nullptr, &v);
}

void parquet_writer::write_float8(std::size_t colidx, double v, bool null) {
    parquet::DoubleWriter* double_writer = static_cast<parquet::DoubleWriter*>(column_writers_[colidx]);
    if (null) {
        int16_t definition_level = 0;
        double_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }
    int16_t definition_level = 1;
    double_writer->WriteBatch(1, &definition_level, nullptr, &v);
}

void parquet_writer::write_character(std::size_t colidx, accessor::text v, bool null) {
    parquet::ByteArrayWriter* char_writer = static_cast<parquet::ByteArrayWriter*>(column_writers_[colidx]);
    if (null) {
        int16_t definition_level = 0;
        char_writer->WriteBatch(1, &definition_level, nullptr, nullptr);
        return;
    }

    parquet::ByteArray value{};
    auto sv = static_cast<std::string_view>(v);
    int16_t definition_level = 1;
    value.ptr = reinterpret_cast<const uint8_t*>(sv.data());
    value.len = sv.size();
    char_writer->WriteBatch(1, &definition_level, nullptr, &value);
}

bool parquet_writer::close() {
    try {
        file_writer_->Close();
        // Write the bytes to file
        DCHECK(fs_->Close().ok());
    } catch (std::exception const& e) {
        VLOG(log_error) << "Parquet write error: " << e.what();
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
            default:
                break;
        }
    }
    return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

}
