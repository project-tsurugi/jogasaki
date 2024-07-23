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
#include "parquet_reader.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iterator>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <boost/assert.hpp>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <glog/logging.h>
#include <parquet/metadata.h>
#include <parquet/types.h>

#include <takatori/datetime/time_of_day.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/file/file_reader.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/octet_field_option.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/utils/decimal.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::string_builder;

template <class T, class Reader>
std::enable_if_t<! std::is_same_v<T, std::int8_t>, T>
read_data(parquet::ColumnReader& reader, parquet::ColumnDescriptor const&, bool& null, bool& nodata) {
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
    throw std::logic_error{"column format error"};
}

template <class T, class Reader>
std::enable_if_t<std::is_same_v<T, std::int8_t>, T>
read_data(parquet::ColumnReader& reader, parquet::ColumnDescriptor const&, bool& null, bool& nodata) {
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
    // T value{};
    bool value{};
    rows_read = r->ReadBatch(1, &definition_level, nullptr, &value, &values_read);
    if(rows_read == 1) {
        if (values_read == 0 && definition_level == 0) {
            null = true;
            return {};
        }
        if (values_read == 1) {
            return static_cast<std::int8_t>(value ? 1 : 0);
        }
    }
    throw std::logic_error{"column format error"};
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
    throw std::logic_error{"column format error"};
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
                throw std::logic_error{"decimal column value error"};
            }
            return utils::read_decimal(buffer, type.type_scale());
        }
    }
    throw std::logic_error{"column format error"};
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

template<>
runtime_t<meta::field_type_kind::time_of_day>
read_data<runtime_t<meta::field_type_kind::time_of_day>, parquet::Int64Reader>(
    parquet::ColumnReader& reader,
    parquet::ColumnDescriptor const& type,
    bool& null,
    bool& nodata
) {
    auto x = read_data<std::int64_t, parquet::Int64Reader>(reader, type, null, nodata);
    return runtime_t<meta::field_type_kind::time_of_day>{std::chrono::nanoseconds{x}};
}

template<>
runtime_t<meta::field_type_kind::time_point>
read_data<runtime_t<meta::field_type_kind::time_point>, parquet::Int64Reader>(
    parquet::ColumnReader& reader,
    parquet::ColumnDescriptor const& type,
    bool& null,
    bool& nodata
) {
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
                case meta::field_type_kind::boolean: ref.set_value<runtime_t<meta::field_type_kind::boolean>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::boolean>, parquet::BoolReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::int4: ref.set_value<runtime_t<meta::field_type_kind::int4>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::int4>, parquet::Int32Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::int8: ref.set_value<runtime_t<meta::field_type_kind::int8>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::int8>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::float4: ref.set_value<runtime_t<meta::field_type_kind::float4>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::float4>, parquet::FloatReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::float8: ref.set_value<runtime_t<meta::field_type_kind::float8>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::float8>, parquet::DoubleReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::decimal: ref.set_value<runtime_t<meta::field_type_kind::decimal>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::decimal>, parquet::ByteArrayReader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::character: ref.set_value<runtime_t<meta::field_type_kind::character>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::character>>(reader, type, null, nodata)); break;
                case meta::field_type_kind::octet: ref.set_value<runtime_t<meta::field_type_kind::octet>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::octet>>(reader, type, null, nodata)); break;
                case meta::field_type_kind::date: ref.set_value<runtime_t<meta::field_type_kind::date>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::date>, parquet::Int32Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::time_of_day: ref.set_value<runtime_t<meta::field_type_kind::time_of_day>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::time_of_day>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                case meta::field_type_kind::time_point: ref.set_value<runtime_t<meta::field_type_kind::time_point>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::time_point>, parquet::Int64Reader>(reader, type, null, nodata)); break;
                default: {
                    VLOG_LP(log_error) << "Parquet reader saw invalid type: " << parameter_meta_->at(i).kind();
                    return false;
                }
            }
            if (nodata) {
                return false;
            }
            ref.set_null(parameter_meta_->nullity_offset(i), null);
        }
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Parquet reader read error: " << e.what();
        return false;
    }
    ++read_count_;
    return true;
}

bool parquet_reader::close() {
    if(file_reader_) {
        try {
            file_reader_->Close();
        } catch (std::exception const& e) {
            VLOG_LP(log_error) << "Parquet close error: " << e.what();
            return false;
        }
        file_reader_.reset();
    }
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

reader_option create_default(meta::record_meta const& meta) {
    std::vector<reader_field_locator> locs{};
    locs.reserve(meta.field_count());
    for(std::size_t i=0, n=meta.field_count(); i < n; ++i) {
        locs.emplace_back("", i);
    }
    return {std::move(locs), meta};
}

std::shared_ptr<parquet_reader> parquet_reader::open(
    std::string_view path,
    reader_option const* opt,
    std::size_t row_group_index
) {
    auto ret = std::make_shared<parquet_reader>();
    if(ret->init(path, opt, row_group_index)) {
        return ret;
    }
    return {};
}

inline constexpr std::string_view to_string_view(parquet::Type::type value) {
    switch (value) {
        case parquet::Type::type::BOOLEAN: return "BOOLEAN";
        case parquet::Type::type::INT32: return "INT32";
        case parquet::Type::type::INT64: return "INT64";
        case parquet::Type::type::INT96: return "INT96";
        case parquet::Type::type::FLOAT: return "FLOAT";
        case parquet::Type::type::DOUBLE: return "DOUBLE";
        case parquet::Type::type::BYTE_ARRAY: return "BYTE_ARRAY";
        case parquet::Type::type::FIXED_LEN_BYTE_ARRAY: return "FIXED_LEN_BYTE_ARRAY";
        case parquet::Type::type::UNDEFINED: return "UNDEFINED";
    }
    std::abort();
}

inline std::ostream& operator<<(std::ostream& out, parquet::Type::type value) {
    return out << to_string_view(value);
}

meta::field_type type(parquet::ColumnDescriptor const* c, meta::field_type* parameter_type) { //NOLINT(readability-function-cognitive-complexity)
    switch(c->logical_type()->type()) {
        case parquet::LogicalType::Type::STRING:
            if (c->physical_type() == parquet::Type::type::BYTE_ARRAY) {
                return meta::field_type{std::make_shared<meta::character_field_option>()};
            }
            break;
        case parquet::LogicalType::Type::INT:
            if(c->logical_type()->Equals(*parquet::LogicalType::Int(8, true)) &&
                c->physical_type() == parquet::Type::type::INT32) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int1>};
            } else if(c->logical_type()->Equals(*parquet::LogicalType::Int(16, true)) &&
                c->physical_type() == parquet::Type::type::INT32) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int2>};
            } else if(c->logical_type()->Equals(*parquet::LogicalType::Int(32, true)) &&
                c->physical_type() == parquet::Type::type::INT32) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int4>};
            } else if(c->logical_type()->Equals(*parquet::LogicalType::Int(64, true)) &&
                c->physical_type() == parquet::Type::type::INT64) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>};
            }
            VLOG_LP(log_error) << "unsupported length for int : " << c->type_length()
                               << " physical type:" << c->physical_type();
            break;
        case parquet::LogicalType::Type::DECIMAL: {
            if (c->physical_type() == parquet::Type::type::BYTE_ARRAY) {
                return meta::field_type{std::make_shared<meta::decimal_field_option>(
                    c->type_precision(),
                    c->type_scale()
                )};
            }
            break;
        }
        case parquet::LogicalType::Type::DATE:
            if (c->physical_type() == parquet::Type::type::INT32) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::date>};
            }
            break;
        case parquet::LogicalType::Type::TIME:
            if (c->physical_type() != parquet::Type::type::INT64) break;
            if(auto t = dynamic_cast<parquet::TimeLogicalType const*>(c->logical_type().get()); t != nullptr) {
                auto with_offset = t->is_adjusted_to_utc();
                return meta::field_type{std::make_shared<meta::time_of_day_field_option>(with_offset)};
            }
            break;
        case parquet::LogicalType::Type::TIMESTAMP:
            if (c->physical_type() != parquet::Type::type::INT64) break;
            if(auto t = dynamic_cast<parquet::TimestampLogicalType const*>(c->logical_type().get()); t != nullptr) {
                auto with_offset = t->is_adjusted_to_utc();
                return meta::field_type{std::make_shared<meta::time_point_field_option>(with_offset)};
            }
            break;
        case parquet::LogicalType::Type::INTERVAL:
            break;
        case parquet::LogicalType::Type::NONE:
            if (c->physical_type() == parquet::Type::type::FLOAT) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::float4>};
            }
            if (c->physical_type() == parquet::Type::type::DOUBLE) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::float8>};
            }
            if (c->physical_type() == parquet::Type::type::BOOLEAN) {
                return meta::field_type{meta::field_enum_tag<meta::field_type_kind::boolean>};
            }
            if (c->physical_type() == parquet::Type::type::BYTE_ARRAY) {
                return meta::field_type{std::make_shared<meta::octet_field_option>()};
            }

            // even without logical type, parameter type helps guessing the type
            if(parameter_type != nullptr) {
                if(c->physical_type() == parquet::Type::type::INT32 &&
                   *parameter_type == meta::field_type{meta::field_enum_tag<meta::field_type_kind::int4>}) {
                    return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int4>};
                }
                if(c->physical_type() == parquet::Type::type::INT64 &&
                   *parameter_type == meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>}) {
                    return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>};
                }
            }
            break;
        default:
            break;
    }
    VLOG_LP(log_debug) << "Column '" << c->name() << "' physical data type '" << c->physical_type()
                       << "' logical data type '" << c->logical_type()->ToString()
                       << "' is not supported and will be ignored.";
    return meta::field_type{meta::field_enum_tag<meta::field_type_kind::undefined>};
}

std::vector<parquet::ColumnDescriptor const*> create_columns_meta(parquet::FileMetaData& pmeta) {
    std::vector<parquet::ColumnDescriptor const*> ret{};
    auto sz = static_cast<std::size_t>(pmeta.schema()->num_columns());
    ret.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        ret.emplace_back(pmeta.schema()->Column(static_cast<int>(i)));
    }
    return ret;
}

meta::field_type parameter_type(
    std::size_t idx,
    meta::record_meta const& parameter_meta,
    std::vector<std::size_t> const& parameter_to_field
) {
    for(std::size_t i=0, n=parameter_to_field.size(); i < n; ++i) {
        if(parameter_to_field[i] == idx) {
            return parameter_meta.at(i);
        }
    }
    return meta::field_type{meta::field_enum_tag<meta::field_type_kind::undefined>};
}

std::shared_ptr<meta::external_record_meta> create_meta(
    parquet::FileMetaData& pmeta,
    meta::record_meta const* parameter_meta,
    std::vector<std::size_t> const* parameter_to_field
) {
    std::vector<std::optional<std::string>> names{};
    std::vector<meta::field_type> types{};
    auto sz = static_cast<std::size_t>(pmeta.schema()->num_columns());
    names.reserve(sz);
    types.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        auto c = pmeta.schema()->Column(static_cast<int>(i));
        names.emplace_back(c->name());
        if(parameter_meta != nullptr) {
            auto p = parameter_type(i, *parameter_meta, *parameter_to_field);
            auto t = type(c, std::addressof(p));
            types.emplace_back(t);
        } else {
            auto t = type(c, nullptr);
            types.emplace_back(t);
        }
    }

    return std::make_shared<meta::external_record_meta>(
        std::make_shared<meta::record_meta>(
            std::move(types),
            boost::dynamic_bitset<std::uint64_t>(sz).flip()
        ),
        std::move(names)
    );
}

bool validate_option(reader_option const& opt, parquet::FileMetaData& pmeta) {
    for(auto&& l : opt.loc_) {
        if(! l.empty_ && l.index_ != npos && static_cast<std::size_t>(pmeta.schema()->num_columns()) <= l.index_) {
            auto msg = string_builder{} <<
                "Reference column index " << l.index_ << " is out of range" << string_builder::to_string;
            VLOG_LP(log_error) << msg;
            return false;
        }
        if(! l.empty_  && l.index_ == npos) {
            bool found = false;
            for(std::size_t i=0, n=pmeta.schema()->num_columns(); i < n; ++i) {
                if(pmeta.schema()->Column(static_cast<int>(i))->name() == l.name_) {
                    found = true;
                    break;
                }
            }
            if(! found) {
                auto msg = string_builder{} <<
                    "Referenced column name " << l.name_ << " not found" << string_builder::to_string;
                VLOG_LP(log_error) << msg;
                return false;
            }
        }
    }
    return true;
}

std::size_t index_in(std::vector<std::string>::value_type const& e, std::vector<std::string>& container) {
    if(auto it = std::find(container.begin(), container.end(), e); it != container.end()) {
        return std::distance(container.begin(), it);
    }
    return npos;
}

std::vector<std::size_t>
create_parameter_to_parquet_field(reader_option const& opt, parquet::FileMetaData& pmeta) {
    std::vector<std::size_t> ret{};
    auto sz = opt.meta_->field_count();
    ret.reserve(sz);
    BOOST_ASSERT(sz == opt.loc_.size()); //NOLINT
    std::vector<std::string> names{};
    names.reserve(pmeta.num_columns());
    for(std::size_t i=0, n=pmeta.num_columns(); i < n; ++i) {
        names.emplace_back(pmeta.schema()->Column(static_cast<int>(i))->name());
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

bool validate_parameter_mapping(
    std::vector<std::size_t> const& param_map,
    meta::record_meta const& parameter_meta,
    meta::external_record_meta const& parquet_meta
) {
    for(std::size_t i=0, n=param_map.size(); i < n; ++i) {
        auto e = param_map[i];
        if(e == npos) continue;
        auto nam = parquet_meta.field_name(e);
        if(parquet_meta.at(e).kind() == meta::field_type_kind::undefined) {
            auto msg = string_builder{} << "Unsupported type - Parquet column '" << (nam.has_value() ? *nam : "") << "'"
                                        << string_builder::to_string;
            VLOG_LP(log_error) << msg;
            return false;
        }
        if(parameter_meta.at(i).kind() != parquet_meta.at(e).kind()) {
            auto msg = string_builder{} <<
                "Invalid parameter type - Parquet column '" << (nam.has_value() ? *nam : "") << "' of type " <<
                parquet_meta.at(e) << " assigned to parameter of type " << parameter_meta.at(i) <<
                string_builder::to_string;
            VLOG_LP(log_error) << msg;
            return false;
        }
    }
    return true;
}

void dump_file_metadata(parquet::FileMetaData& pmeta) {
    VLOG_LP(log_debug) << "*** begin dump metadata for parquet file ***";
    VLOG_LP(log_debug) << "size:" << pmeta.size();
    VLOG_LP(log_debug) << "num_rows:" << pmeta.num_rows();
    VLOG_LP(log_debug) << "created_by:" << pmeta.created_by();

    VLOG_LP(log_debug) << "num_row_groups:" << pmeta.num_row_groups();
    for(std::size_t i=0, n=pmeta.num_row_groups(); i<n; ++i) {
        auto rg = pmeta.RowGroup(static_cast<int>(i));
        VLOG_LP(log_debug) << "  RowGroup:" << i << " num_rows:" << rg->num_rows()
                           << " total_byte_size:" << rg->total_byte_size()
                           << " total_compressed_size:" << rg->total_compressed_size();
    }
    VLOG_LP(log_debug) << "schema name:" << pmeta.schema()->name();
    VLOG_LP(log_debug) << "num_columns:" << pmeta.num_columns();
    // encodings can be different among row groups, but we don't use such format so often, so simply display fixed rg.
    auto rg = pmeta.RowGroup(0);
    for(std::size_t i=0, n=pmeta.schema()->num_columns(); i<n; ++i) {
        auto&& c = pmeta.schema()->Column(static_cast<int>(i));
        auto&& cm = rg->ColumnChunk(static_cast<int>(i));
        std::stringstream ss{};
        ss << "  column name:" << c->name() << " physical type:" << c->physical_type()
           << " logical type:" << c->logical_type()->ToString();
        ss << " encodings:[";
        for(auto&& enc : cm->encodings()) {
            ss << " ";
            ss << parquet::EncodingToString(enc);
        }
        ss << " ]";
        VLOG_LP(log_debug) << ss.str();
    }

    VLOG_LP(log_debug) << "*** end dump metadata for parquet file ***";
}

bool parquet_reader::init(
    std::string_view path,
    reader_option const* opt,
    std::size_t row_group_index
) {
    try {
        path_ = std::string{path};
        file_reader_ = parquet::ParquetFileReader::OpenFile(path_.string(), false);
        auto file_metadata = file_reader_->metadata();
        dump_file_metadata(*file_metadata);
        row_group_count_ = file_metadata->num_row_groups();
        if(row_group_index != index_unspecified && row_group_index >= row_group_count_) {
            VLOG_LP(log_error) << "row group index:" << row_group_index <<
                    " too large for row group count:" << row_group_count_;
            return false;
        }
        row_group_index_ = row_group_index == index_unspecified ? 0 : row_group_index;
        if(opt != nullptr) {
            parameter_meta_ = maybe_shared_ptr{opt->meta_};
            if(! validate_option(*opt, *file_metadata)) {
                return false;
            }
            parameter_to_parquet_field_ = create_parameter_to_parquet_field(*opt, *file_metadata);
            meta_ = create_meta(*file_metadata, parameter_meta_.get(), std::addressof(parameter_to_parquet_field_));
            if(! validate_parameter_mapping(parameter_to_parquet_field_, *parameter_meta_, *meta_)) {
                return false;
            }
        } else {
            // this is for testing - create mock option
            meta_ = create_meta(*file_metadata, nullptr, nullptr);
            reader_option d = create_default(*meta_->origin());
            parameter_meta_ = maybe_shared_ptr{d.meta_};
            parameter_to_parquet_field_ = create_parameter_to_parquet_field(d, *file_metadata);
        }
        if(! meta_) {
            return false;
        }

        columns_ = create_columns_meta(*file_metadata);
        buf_ = data::aligned_buffer{parameter_meta_->record_size(), parameter_meta_->record_alignment()};
        buf_.resize(parameter_meta_->record_size());
        row_group_reader_ = file_reader_->RowGroup(static_cast<int>(row_group_index_));
        column_readers_.reserve(meta_->field_count());
        for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
            column_readers_.emplace_back(row_group_reader_->Column(static_cast<int>(i)));
        }
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Parquet reader init error: " << e.what();
        return false;
    }
    return true;
}

std::size_t parquet_reader::row_group_count() const noexcept {
    return row_group_count_;
}

parquet_reader::~parquet_reader() noexcept {
    close();
}

}  // namespace jogasaki::executor::file
