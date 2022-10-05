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
#include "parquet_reader.h"

#include <iomanip>
#include <glog/logging.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>

#include <jogasaki/logging.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/utils/decimal.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;

template <class T, class Reader>
T read_data(parquet::ColumnReader& reader, parquet::ColumnDescriptor const&, bool& null, bool& nodata) {
    int64_t values_read = 0;
    int64_t rows_read = 0;
    int16_t definition_level = 0;
    null = false;
    nodata = false;
    auto* r = static_cast<Reader*>(std::addressof(reader));
    if(! r->HasNext()) {
        nodata = true;
        return {};
    }
    T value{};
    rows_read = r->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
    if(rows_read == 1) {
        if (values_read == 0 && definition_level == 0) {
            null = true;
            return {};
        }
        if (values_read == 1) {
            return value;
        }
    }
    fail();
}

template <>
accessor::text read_data<accessor::text, parquet::ByteArrayReader>(parquet::ColumnReader& reader, parquet::ColumnDescriptor const&, bool& null, bool& nodata) {
    int64_t values_read = 0;
    int64_t rows_read = 0;
    int16_t definition_level = 0;
    null = false;
    nodata = false;
    auto* r = static_cast<parquet::ByteArrayReader*>(std::addressof(reader));  //NOLINT
    if(! r->HasNext()) {
        nodata = true;
        return {};
    }

    parquet::ByteArray value{};
    rows_read = r->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
    if(rows_read == 1) {
        if (values_read == 0 && definition_level == 0) {
            null = true;
            return {};
        }
        if (values_read == 1) {
            return accessor::text{reinterpret_cast<char const*>(value.ptr), value.len};  //NOLINT
        }
    }
    fail();
}

template <>
runtime_t<meta::field_type_kind::decimal>
read_data<runtime_t<meta::field_type_kind::decimal>, parquet::ByteArrayReader>(
    parquet::ColumnReader& reader,
    parquet::ColumnDescriptor const& type,
    bool& null,
    bool& nodata
) {
    int64_t values_read = 0;
    int64_t rows_read = 0;
    int16_t definition_level = 0;
    null = false;
    nodata = false;
    auto* r = static_cast<parquet::ByteArrayReader*>(std::addressof(reader));  //NOLINT
    if(! r->HasNext()) {
        nodata = true;
        return {};
    }

    parquet::ByteArray value{};
    rows_read = r->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
    if(rows_read == 1) {
        if (values_read == 0 && definition_level == 0) {
            null = true;
            return {};
        }
        if (values_read == 1) {
            std::string_view buffer{reinterpret_cast<char const*>(value.ptr), value.len};  //NOLINT
            if(! utils::validate_decimal_coefficient(buffer)) {
                fail();
            }
            return utils::read_decimal(buffer, type.type_scale());
        }
    }
    fail();
}

template <>
runtime_t<meta::field_type_kind::date> read_data<runtime_t<meta::field_type_kind::date>, parquet::Int32Reader>(
    parquet::ColumnReader& reader,
    parquet::ColumnDescriptor const& type,
    bool& null,
    bool& nodata) {
        auto x = read_data<std::int32_t, parquet::Int32Reader>(reader, type, null, nodata);
        return runtime_t<meta::field_type_kind::date>{x};
}

template <>
runtime_t<meta::field_type_kind::time_of_day> read_data<runtime_t<meta::field_type_kind::time_of_day>, parquet::Int64Reader>(
    parquet::ColumnReader& reader,
    parquet::ColumnDescriptor const& type,
    bool& null,
    bool& nodata) {
        auto x = read_data<std::int64_t, parquet::Int64Reader>(reader, type, null, nodata);
        return runtime_t<meta::field_type_kind::time_of_day>{std::chrono::nanoseconds{x}};
}

template <>
runtime_t<meta::field_type_kind::time_point> read_data<runtime_t<meta::field_type_kind::time_point>, parquet::Int64Reader>(
    parquet::ColumnReader& reader,
    parquet::ColumnDescriptor const& type,
    bool& null,
    bool& nodata) {
    auto x = read_data<std::int64_t, parquet::Int64Reader>(reader, type, null, nodata);
    return runtime_t<meta::field_type_kind::time_point>{std::chrono::nanoseconds{x}};
}

bool parquet_reader::next(accessor::record_ref& ref) {
    ref = accessor::record_ref{buf_.data(), buf_.capacity()};
    try {
        for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
            auto& reader = *column_readers_[i];
            auto& type = *columns_[i];
            bool null{false};
            bool nodata{false};
            switch(meta_->at(i).kind()) {
                case meta::field_type_kind::int4: ref.set_value<runtime_t<meta::field_type_kind::int4>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::int4>, parquet::Int32Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::int8: ref.set_value<runtime_t<meta::field_type_kind::int8>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::int8>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::float4: ref.set_value<runtime_t<meta::field_type_kind::float4>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::float4>, parquet::FloatReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::float8: ref.set_value<runtime_t<meta::field_type_kind::float8>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::float8>, parquet::DoubleReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::character: ref.set_value<runtime_t<meta::field_type_kind::character>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::character>, parquet::ByteArrayReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::decimal: ref.set_value<runtime_t<meta::field_type_kind::decimal>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::decimal>, parquet::ByteArrayReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::date: ref.set_value<runtime_t<meta::field_type_kind::date>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::date>, parquet::Int32Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::time_of_day: ref.set_value<runtime_t<meta::field_type_kind::time_of_day>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::time_of_day>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::time_point: ref.set_value<runtime_t<meta::field_type_kind::time_point>>(meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::time_point>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                default: fail();
            }
            if (nodata) {
                return false;
            }
            ref.set_null(meta_->nullity_offset(i), null);
        }
    } catch (std::exception const& e) {
        VLOG(log_error) << "Parquet reader read error: " << e.what();
        return false;
    }
    ++read_count_;
    return true;
}

bool parquet_reader::close() {
    file_reader_->Close();
    return true;
}

std::string parquet_reader::path() const noexcept {
    return path_.string();
}

std::size_t parquet_reader::read_count() const noexcept {
    return read_count_;
}

maybe_shared_ptr<meta::external_record_meta> const& parquet_reader::meta() {
    return meta_;
}

std::shared_ptr<parquet_reader> parquet_reader::open(std::string_view path) {
    auto ret = std::make_shared<parquet_reader>();
    if(ret->init(path)) {
        return ret;
    }
    return {};
}

meta::field_type type(parquet::ColumnDescriptor const* c) {
    if (c->physical_type() == parquet::Type::type::FLOAT) {
        return meta::field_type{meta::field_enum_tag<meta::field_type_kind::float4>};
    }
    if (c->physical_type() == parquet::Type::type::DOUBLE) {
        return meta::field_type{meta::field_enum_tag<meta::field_type_kind::float8>};
    }
    switch(c->logical_type()->type()) {
        case parquet::LogicalType::Type::STRING:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::character>};
        case parquet::LogicalType::Type::INT:
            if(c->logical_type()->Equals(*parquet::LogicalType::Int(32, true))) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int4>};
            } else if(c->logical_type()->Equals(*parquet::LogicalType::Int(64, true))) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>};
            }
            std::cerr << " length " << c->type_length() << std::endl;
            fail();
        case parquet::LogicalType::Type::DECIMAL: {
            return meta::field_type{std::make_shared<meta::decimal_field_option>(
                c->type_precision(),
                c->type_scale()
            )};
        }
        case parquet::LogicalType::Type::NIL:
        case parquet::LogicalType::Type::NONE:
        case parquet::LogicalType::Type::DATE:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::date>};
        case parquet::LogicalType::Type::TIME:
            return meta::field_type{std::make_shared<meta::time_of_day_field_option>()};
        case parquet::LogicalType::Type::TIMESTAMP:
            return meta::field_type{std::make_shared<meta::time_point_field_option>()};
        case parquet::LogicalType::Type::INTERVAL:
            fail();
        default:
            fail();
    }
}

std::vector<parquet::ColumnDescriptor const*> create_columns_meta(parquet::FileMetaData& pmeta) {
    std::vector<parquet::ColumnDescriptor const*> ret{};
    auto sz = static_cast<std::size_t>(pmeta.schema()->num_columns());
    ret.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        ret.emplace_back(pmeta.schema()->Column(i));
    }
    return ret;
}

std::shared_ptr<meta::external_record_meta> create_meta(parquet::FileMetaData& pmeta) {
    std::vector<std::optional<std::string>> names{};
    std::vector<meta::field_type> types{};
    auto sz = static_cast<std::size_t>(pmeta.schema()->num_columns());
    names.reserve(sz);
    types.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        auto c = pmeta.schema()->Column(i);
        names.emplace_back(c->name());
        types.emplace_back(type(c));
    }

    return std::make_shared<meta::external_record_meta>(
        std::make_shared<meta::record_meta>(
            std::move(types),
            boost::dynamic_bitset<std::uint64_t>(sz).flip()
        ),
        std::move(names)
    );
}

bool parquet_reader::init(std::string_view path) {
    try {
        path_ = std::string{path};
        file_reader_ = parquet::ParquetFileReader::OpenFile(path_.string(), false);
        auto file_metadata = file_reader_->metadata();
        if(file_metadata->num_row_groups() != 1) {
            VLOG(log_error) << "parquet file format error : more than one row groups";
            return false;
        }
        meta_ = create_meta(*file_metadata);
        columns_ = create_columns_meta(*file_metadata);
        buf_ = data::aligned_buffer{meta_->record_size(), meta_->record_alignment()};
        buf_.resize(meta_->record_size());
        row_group_reader_ = file_reader_->RowGroup(0);
        column_readers_.reserve(meta_->field_count());
        for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
            column_readers_.emplace_back(row_group_reader_->Column(i));
        }
    } catch (std::exception const& e) {
        VLOG(log_error) << "Parquet reader init error: " << e.what();
        return false;
    }
    return true;
}
}
