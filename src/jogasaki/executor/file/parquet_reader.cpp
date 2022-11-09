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
#include <algorithm>
#include <glog/logging.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/logging.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/utils/decimal.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;
using takatori::util::string_builder;

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

template <class T>
std::enable_if_t<std::is_same_v<T, accessor::text> || std::is_same_v<T, accessor::binary>, T>
read_data(parquet::ColumnReader& reader, parquet::ColumnDescriptor const&, bool& null, bool& nodata) {
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
            return T{reinterpret_cast<char const*>(value.ptr), value.len};  //NOLINT
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
        auto sz = parameter_to_parquet_field_.size();
        for(std::size_t i=0; i<sz; ++i) {
            auto colidx = parameter_to_parquet_field_[i];
            if(colidx == npos) continue;
            auto& reader = *column_readers_[colidx];
            auto& type = *columns_[colidx];
            bool null{false};
            bool nodata{false};
            switch(parameter_meta_->at(i).kind()) {
                case meta::field_type_kind::int4: ref.set_value<runtime_t<meta::field_type_kind::int4>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::int4>, parquet::Int32Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::int8: ref.set_value<runtime_t<meta::field_type_kind::int8>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::int8>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::float4: ref.set_value<runtime_t<meta::field_type_kind::float4>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::float4>, parquet::FloatReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::float8: ref.set_value<runtime_t<meta::field_type_kind::float8>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::float8>, parquet::DoubleReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::character: ref.set_value<runtime_t<meta::field_type_kind::character>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::character>>(reader, type, null, nodata)); break;
                case meta::field_type_kind::octet: ref.set_value<runtime_t<meta::field_type_kind::octet>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::octet>>(reader, type, null, nodata)); break;
                case meta::field_type_kind::decimal: ref.set_value<runtime_t<meta::field_type_kind::decimal>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::decimal>, parquet::ByteArrayReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::date: ref.set_value<runtime_t<meta::field_type_kind::date>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::date>, parquet::Int32Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::time_of_day: ref.set_value<runtime_t<meta::field_type_kind::time_of_day>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::time_of_day>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::time_point: ref.set_value<runtime_t<meta::field_type_kind::time_point>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::time_point>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                default: fail();
            }
            if (nodata) {
                return false;
            }
            ref.set_null(parameter_meta_->nullity_offset(i), null);
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

parquet_reader_option create_default(meta::record_meta const& meta) {
    std::vector<parquet_reader_field_locator> locs{};
    locs.reserve(meta.field_count());
    for(std::size_t i=0, n=meta.field_count(); i < n; ++i) {
        locs.emplace_back("", i);
    }
    return {std::move(locs), meta};
}

std::shared_ptr<parquet_reader> parquet_reader::open(std::string_view path, parquet_reader_option const* opt) {
    auto ret = std::make_shared<parquet_reader>();
    if(ret->init(path, opt)) {
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
    if (c->physical_type() == parquet::Type::type::BOOLEAN) {
        return meta::field_type{meta::field_enum_tag<meta::field_type_kind::boolean>};
    }
    switch(c->logical_type()->type()) {
        case parquet::LogicalType::Type::STRING:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::character>};
        case parquet::LogicalType::Type::INT:
            if(c->logical_type()->Equals(*parquet::LogicalType::Int(8, true))) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int1>};
            } else if(c->logical_type()->Equals(*parquet::LogicalType::Int(16, true))) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int2>};
            } else if(c->logical_type()->Equals(*parquet::LogicalType::Int(32, true))) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int4>};
            } else if(c->logical_type()->Equals(*parquet::LogicalType::Int(64, true))) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>};
            }
            VLOG(log_error) << "unsupported length for int : " << c->type_length();
            break;
        case parquet::LogicalType::Type::DECIMAL: {
            return meta::field_type{std::make_shared<meta::decimal_field_option>(
                c->type_precision(),
                c->type_scale()
            )};
        }
        case parquet::LogicalType::Type::DATE:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::date>};
        case parquet::LogicalType::Type::TIME:
            if(auto t = dynamic_cast<parquet::TimeLogicalType const*>(c->logical_type().get()); t != nullptr) {
                auto with_offset = t->is_adjusted_to_utc();
                return meta::field_type{std::make_shared<meta::time_of_day_field_option>(with_offset)};
            }
            break;
        case parquet::LogicalType::Type::TIMESTAMP:
            if(auto t = dynamic_cast<parquet::TimestampLogicalType const*>(c->logical_type().get()); t != nullptr) {
                auto with_offset = t->is_adjusted_to_utc();
                return meta::field_type{std::make_shared<meta::time_point_field_option>(with_offset)};
            }
            break;
        case parquet::LogicalType::Type::INTERVAL:
            break;
        case parquet::LogicalType::Type::NONE:
            if (c->physical_type() == parquet::Type::type::BYTE_ARRAY) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::octet>};
            }
            break;
        default:
            break;
    }
    VLOG(log_error) << "Column '" << c->name() << "' data type '" << c->logical_type()->ToString() << "' is not supported.";
    return meta::field_type{meta::field_enum_tag<meta::field_type_kind::undefined>};
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
        auto t = type(c);
        if(t.kind() == meta::field_type_kind::undefined) {
            // unsupported type
            return {};
        }
        types.emplace_back(t);
    }

    return std::make_shared<meta::external_record_meta>(
        std::make_shared<meta::record_meta>(
            std::move(types),
            boost::dynamic_bitset<std::uint64_t>(sz).flip()
        ),
        std::move(names)
    );
}

bool validate_option(parquet_reader_option const& opt, parquet::FileMetaData& pmeta) {
    for(auto&& l : opt.loc_) {
        if(! l.empty_ && l.index_ != npos && static_cast<std::size_t>(pmeta.schema()->num_columns()) <= l.index_) {
            auto msg = string_builder{} <<
                "Reference column index " << l.index_ << " is out of range" << string_builder::to_string;
            VLOG(log_error) << msg;
            return false;
        }
        if(! l.empty_  && l.index_ == npos) {
            bool found = false;
            for(std::size_t i=0, n=pmeta.schema()->num_columns(); i < n; ++i) {
                if(pmeta.schema()->Column(i)->name() == l.name_) {
                    found = true;
                    break;
                }
            }
            if(! found) {
                auto msg = string_builder{} <<
                    "Referenced column name " << l.name_ << " not found" << string_builder::to_string;
                VLOG(log_error) << msg;
                return false;
            }
        }
    }
    return true;
}

std::size_t index_in(std::vector<std::string>::value_type e, std::vector<std::string>& container) {
    if(auto it = std::find(container.begin(), container.end(), e); it != container.end()) {
        return std::distance(container.begin(), it);
    }
    return npos;
}

std::vector<std::size_t> create_parameter_to_parquet_field(parquet_reader_option const& opt, parquet::FileMetaData& pmeta) {
    std::vector<std::size_t> ret{};
    auto sz = opt.meta_->field_count();
    ret.reserve(sz);
    BOOST_ASSERT(sz == opt.loc_.size()); //NOLINT
    std::vector<std::string> names{};
    names.reserve(pmeta.num_columns());
    for(std::size_t i=0, n=pmeta.num_columns(); i < n; ++i) {
        names.emplace_back(pmeta.schema()->Column(i)->name());
    }

    for(std::size_t i=0; i < sz; ++i) {
        if(opt.loc_[i].empty_) {
            ret.emplace_back(npos);
            continue;
        }
        if(opt.loc_[i].index_ != npos) {
            ret.emplace_back(opt.loc_[i].index_);
            continue;
        }
        if(auto idx = index_in(opt.loc_[i].name_, names); idx != npos) {
            ret.emplace_back(idx);
            continue;
        }
        // something is wrong
    }
    return ret;
}

bool parquet_reader::init(std::string_view path, parquet_reader_option const* opt) {
    try {
        path_ = std::string{path};
        file_reader_ = parquet::ParquetFileReader::OpenFile(path_.string(), false);
        auto file_metadata = file_reader_->metadata();
        if(file_metadata->num_row_groups() != 1) {
            VLOG(log_error) << "parquet file format error : more than one row groups";
            return false;
        }
        meta_ = create_meta(*file_metadata);
        if(! meta_) {
            return false;
        }
        parquet_reader_option d = create_default(*meta_->origin());
        if(opt == nullptr) {
            opt = &d;
        }
        parameter_meta_ = maybe_shared_ptr{opt->meta_};
        if(! validate_option(*opt, *file_metadata)) {
            return false;
        }
        parameter_to_parquet_field_ = create_parameter_to_parquet_field(*opt, *file_metadata);
        columns_ = create_columns_meta(*file_metadata);
        buf_ = data::aligned_buffer{parameter_meta_->record_size(), parameter_meta_->record_alignment()};
        buf_.resize(parameter_meta_->record_size());
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
