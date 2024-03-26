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
#include "arrow_reader.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iterator>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <arrow/array.h>
#include <arrow/array/array_base.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/basic_decimal.h>
#include <arrow/util/decimal.h>
#include <arrow/util/string_builder.h>
#include <boost/assert.hpp>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <glog/logging.h>

#include <takatori/datetime/time_of_day.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/exception.h>
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
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/utils/finally.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::string_builder;
using takatori::util::throw_exception;

template <class T, arrow::Type::type Typeid>
void validate_ctype() {
    using c_type = typename arrow::TypeTraits<typename arrow::TypeIdTraits<Typeid>::Type>::CType;
    static_assert(std::is_same_v<c_type, T>);
}

// INT8, INT16, INT32, INT64, FLOAT, DOUBLE
template <class T, arrow::Type::type Typeid>
std::enable_if_t<
std::is_same_v<T, std::int32_t> ||
std::is_same_v<T, std::int64_t> ||
std::is_same_v<T, float> ||
std::is_same_v<T, double>
, T>
read_data(arrow::Array& array, std::size_t offset) {
    validate_ctype<T, Typeid>();
    using array_type = typename arrow::TypeTraits<typename arrow::TypeIdTraits<Typeid>::Type>::ArrayType;
    auto& r = static_cast<array_type&>(array);
    return r.Value(offset);
}

template <class T, arrow::Type::type Typeid>
std::enable_if_t<std::is_same_v<T, accessor::text> || std::is_same_v<T, accessor::binary>, T>
read_data(arrow::Array& array, std::size_t offset) {
    using array_type = typename arrow::TypeTraits<typename arrow::TypeIdTraits<Typeid>::Type>::ArrayType;
    auto& r = static_cast<array_type&>(array);
    return accessor::text{r.GetView(offset)};
}

template <class T, arrow::Type::type Typeid>
std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::decimal>> && Typeid == arrow::Type::DECIMAL, T>
read_data(arrow::Array& array, std::size_t offset) {
    using array_type = typename arrow::TypeTraits<typename arrow::TypeIdTraits<Typeid>::Type>::ArrayType;
    auto& r = static_cast<array_type&>(array);
    auto scale = static_cast<arrow::Decimal128Type&>(*r.type()).scale();
    auto* ptr = r.Value(offset);
    std::int64_t high{};
    std::uint64_t low{};
    std::memcpy(&low, ptr, 8);
    std::memcpy(&high, ptr + 8, 8);  //NOLINT
    arrow::Decimal128 d{high, low};
    bool negative = false;
    if(d < 0) {
        d.Abs();
        negative = true;
    }
    auto h = d.high_bits();
    auto l = d.low_bits();
    return takatori::decimal::triple{
        negative ? -1 : +1,
        static_cast<std::uint64_t>(h),
        l,
        -static_cast<std::int32_t>(scale),
    };
}

template <class T, arrow::Type::type Typeid>
std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::date>> && Typeid == arrow::Type::DATE32, T>
read_data(arrow::Array& array, std::size_t offset) {
    using array_type = typename arrow::TypeTraits<typename arrow::TypeIdTraits<Typeid>::Type>::ArrayType;
    auto& r = static_cast<array_type&>(array);
    auto x = r.Value(offset);
    return runtime_t<meta::field_type_kind::date>{x};
}

template <class T, arrow::Type::type Typeid>
std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::time_of_day>> && Typeid == arrow::Type::TIME64, T>
read_data(arrow::Array& array, std::size_t offset) {
    using array_type = typename arrow::TypeTraits<typename arrow::TypeIdTraits<Typeid>::Type>::ArrayType;
    auto& r = static_cast<array_type&>(array);
    auto x = r.Value(offset);
    return runtime_t<meta::field_type_kind::time_of_day>{std::chrono::nanoseconds{x}};
}

template <class T, arrow::Type::type Typeid>
std::enable_if_t<std::is_same_v<T, runtime_t<meta::field_type_kind::time_point>> && Typeid == arrow::Type::TIMESTAMP, T>
read_data(arrow::Array& array, std::size_t offset) {
    using array_type = typename arrow::TypeTraits<typename arrow::TypeIdTraits<Typeid>::Type>::ArrayType;
    auto& r = static_cast<array_type&>(array);
    auto x = r.Value(offset);
    return runtime_t<meta::field_type_kind::time_point>{std::chrono::nanoseconds{x}};
}

bool arrow_reader::next(accessor::record_ref& ref) {
    ref = accessor::record_ref{buf_.data(), buf_.capacity()};
    if(static_cast<std::int64_t>(offset_) >= record_batch_->num_rows()) {
        return false;
    }
    utils::finally finalizer([&]() {
        ++offset_;
    });
    try {
        auto sz = parameter_to_field_.size();
        for(std::size_t i=0; i<sz; ++i) {
            auto colidx = parameter_to_field_[i];
            if(colidx == npos) continue;
            auto& array = *record_batch_->column(static_cast<int>(colidx));
            auto& field = record_batch_->schema()->field(static_cast<int>(i));
            bool null = array.IsNull(static_cast<int>(offset_));
            ref.set_null(parameter_meta_->nullity_offset(static_cast<int>(i)), null);
            if(null) continue;
            switch(field->type()->id()) {
                case arrow::Type::INT32: ref.set_value<runtime_t<meta::field_type_kind::int4>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::int4>, arrow::Type::INT32>(array, offset_)); break;
                case arrow::Type::INT64: ref.set_value<runtime_t<meta::field_type_kind::int8>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::int8>, arrow::Type::INT64>(array, offset_)); break;
                case arrow::Type::FLOAT: ref.set_value<runtime_t<meta::field_type_kind::float4>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::float4>, arrow::Type::FLOAT>(array, offset_)); break;
                case arrow::Type::DOUBLE: ref.set_value<runtime_t<meta::field_type_kind::float8>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::float8>, arrow::Type::DOUBLE>(array, offset_)); break;
                case arrow::Type::STRING: ref.set_value<runtime_t<meta::field_type_kind::character>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::character>, arrow::Type::STRING>(array, offset_)); break;
                case arrow::Type::DATE32: ref.set_value<runtime_t<meta::field_type_kind::date>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::date>, arrow::Type::DATE32>(array, offset_)); break;
                case arrow::Type::TIME64: ref.set_value<runtime_t<meta::field_type_kind::time_of_day>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::time_of_day>, arrow::Type::TIME64>(array, offset_)); break;
                case arrow::Type::TIMESTAMP: ref.set_value<runtime_t<meta::field_type_kind::time_point>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::time_point>, arrow::Type::TIMESTAMP>(array, offset_)); break;
                case arrow::Type::DECIMAL128: ref.set_value<runtime_t<meta::field_type_kind::decimal>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::decimal>, arrow::Type::DECIMAL128>(array, offset_)); break;
                case arrow::Type::FIXED_SIZE_BINARY: ref.set_value<runtime_t<meta::field_type_kind::character>>(parameter_meta_->value_offset(i), read_data<runtime_t<meta::field_type_kind::character>, arrow::Type::FIXED_SIZE_BINARY>(array, offset_)); break;
                default: {
                    VLOG_LP(log_error) << "Arrow array saw invalid type: " << parameter_meta_->at(i).kind();
                    return false;
                }
            }
        }
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Arrow array read error: " << e.what();
        return false;
    }
    ++read_count_;
    return true;
}

bool arrow_reader::close() {
    // FIXME need closing arrow ipc files/streams?
    if(input_file_) {
        try {
            auto res = input_file_->Close();
            if(! res.ok()) {
                VLOG_LP(log_error) << "Arrow close error: " << res;
                return false;
            }
        } catch (std::exception const& e) {
            VLOG_LP(log_error) << "Arrow close error: " << e.what();
            return false;
        }
        input_file_.reset();
    }
    return true;
}

std::string arrow_reader::path() const noexcept {
    return path_.string();
}

std::size_t arrow_reader::read_count() const noexcept {
    return read_count_;
}

maybe_shared_ptr<meta::external_record_meta> const& arrow_reader::meta() {
    return meta_;
}

static reader_option create_default(meta::record_meta const& meta) {
    std::vector<reader_field_locator> locs{};
    locs.reserve(meta.field_count());
    for(std::size_t i=0, n=meta.field_count(); i < n; ++i) {
        locs.emplace_back("", i);
    }
    return {std::move(locs), meta};
}

std::shared_ptr<arrow_reader> arrow_reader::open(
    std::string_view path,
    reader_option const* opt,
    std::size_t row_group_index
) {
    auto ret = std::make_shared<arrow_reader>();
    if(ret->init(path, opt, row_group_index)) {
        return ret;
    }
    return {};
}

static meta::field_type type(arrow::Field& c, meta::field_type* parameter_type) { //NOLINT(readability-function-cognitive-complexity)
    (void) parameter_type;
    switch(c.type()->id()) {
        case arrow::Type::INT8:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int1>};
        case arrow::Type::INT16:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int2>};
        case arrow::Type::INT32:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int4>};
        case arrow::Type::INT64:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>};
        case arrow::Type::FLOAT:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::float4>};
        case arrow::Type::DOUBLE:
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::float8>};
        case arrow::Type::STRING:
            return meta::field_type{std::make_shared<meta::character_field_option>()};
        case arrow::Type::DECIMAL128: {
            auto p = static_cast<arrow::Decimal128Type&>(*c.type()).precision();  //NOLINT
            auto s = static_cast<arrow::Decimal128Type&>(*c.type()).scale();  //NOLINT
            return meta::field_type{std::make_shared<meta::decimal_field_option>(p, s)};
        }
        case arrow::Type::DATE32: {
            if(static_cast<arrow::DateType&>(*c.type()).unit() != arrow::DateUnit::DAY) {  //NOLINT
                VLOG_LP(log_warning) << "Column '" << c.name() << "' data type '" << c.type()->ToString()
                                   << "' has non-day date unit and will be ignored.";
                break;
            }
            return meta::field_type{meta::field_enum_tag<meta::field_type_kind::date>};
        }
        case arrow::Type::TIME64: {
            if(static_cast<arrow::Time64Type&>(*c.type()).unit() != arrow::TimeUnit::NANO) { //NOLINT
                VLOG_LP(log_warning) << "Column '" << c.name() << "' data type '" << c.type()->ToString()
                                   << "' has non-nano time unit and will be ignored.";
                break;
            }
            return meta::field_type{std::make_shared<meta::time_of_day_field_option>()}; // TODO with offset
        }
        case arrow::Type::TIMESTAMP: {
            auto& typ = static_cast<arrow::TimestampType&>(*c.type());  //NOLINT
            if(typ.unit() != arrow::TimeUnit::NANO) {
                VLOG_LP(log_warning) << "Column '" << c.name() << "' data type '" << c.type()->ToString()
                                   << "' has non-nano time unit and will be ignored.";
                break;
            }
            if(! typ.timezone().empty() && typ.timezone() != "UTC") {
                VLOG_LP(log_warning) << "Column '" << c.name() << "' data type '" << c.type()->ToString()
                                   << "' has non-UTC timezone and will be ignored.";
                break;
            }
            return meta::field_type{std::make_shared<meta::time_point_field_option>(! typ.timezone().empty())};
        }
        case arrow::Type::FIXED_SIZE_BINARY: {
            auto& typ = static_cast<arrow::FixedSizeBinaryType&>(*c.type());  //NOLINT
            return meta::field_type{std::make_shared<meta::character_field_option>(false, typ.byte_width())};
        }
        default:
            break;
    }
    VLOG_LP(log_debug) << "Column '" << c.name() << "' data type '" << c.type()->ToString()
                       << "' is not supported and will be ignored.";
    return meta::field_type{meta::field_enum_tag<meta::field_type_kind::undefined>};
}

static meta::field_type parameter_type(
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

static std::shared_ptr<meta::external_record_meta> create_meta(
    arrow::Schema& schema,
    meta::record_meta const* parameter_meta,
    std::vector<std::size_t> const* parameter_to_field
) {
    std::vector<std::optional<std::string>> names{};
    std::vector<meta::field_type> types{};
    auto sz = static_cast<std::size_t>(schema.num_fields());
    names.reserve(sz);
    types.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        auto& c = schema.field(static_cast<int>(i));
        names.emplace_back(c->name());
        if(parameter_meta != nullptr) {
            auto p = parameter_type(i, *parameter_meta, *parameter_to_field);
            auto t = type(*c, std::addressof(p));
            types.emplace_back(t);
        } else {
            auto t = type(*c, nullptr);
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

static bool validate_option(reader_option const& opt, arrow::Schema& schema) {
    for(auto&& l : opt.loc_) {
        if(! l.empty_ && l.index_ != npos && static_cast<std::size_t>(schema.num_fields()) <= l.index_) {
            auto msg = string_builder{} <<
                "Reference column index " << l.index_ << " is out of range" << string_builder::to_string;
            VLOG_LP(log_error) << msg;
            return false;
        }
        if(! l.empty_  && l.index_ == npos) {
            bool found = false;
            for(std::size_t i=0, n=schema.num_fields(); i < n; ++i) {
                if(schema.field(static_cast<int>(i))->name() == l.name_) {
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

static std::size_t index_in(std::vector<std::string>::value_type const& e, std::vector<std::string>& container) {
    if(auto it = std::find(container.begin(), container.end(), e); it != container.end()) {
        return std::distance(container.begin(), it);
    }
    return npos;
}

static std::vector<std::size_t>
create_parameter_to_field(reader_option const& opt, arrow::Schema& schema) {
    std::vector<std::size_t> ret{};
    auto sz = opt.meta_->field_count();
    ret.reserve(sz);
    BOOST_ASSERT(sz == opt.loc_.size()); //NOLINT
    std::vector<std::string> names{};
    names.reserve(schema.num_fields());
    for(std::size_t i=0, n=schema.num_fields(); i < n; ++i) {
        names.emplace_back(schema.field(static_cast<int>(i))->name());
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

static bool validate_parameter_mapping(
    std::vector<std::size_t> const& param_map,
    meta::record_meta const& parameter_meta,
    meta::external_record_meta const& external_meta
) {
    for(std::size_t i=0, n=param_map.size(); i < n; ++i) {
        auto e = param_map[i];
        if(e == npos) continue;
        auto nam = external_meta.field_name(e);
        if(external_meta.at(e).kind() == meta::field_type_kind::undefined) {
            auto msg = string_builder{} << "Unsupported type - Arrow column '" << (nam.has_value() ? *nam : "") << "'"
                                        << string_builder::to_string;
            VLOG_LP(log_error) << msg;
            return false;
        }
        if(parameter_meta.at(i).kind() != external_meta.at(e).kind()) {
            auto msg = string_builder{} <<
                "Invalid parameter type - Arrow column '" << (nam.has_value() ? *nam : "") << "' of type " <<
                external_meta.at(e) << " assigned to parameter of type " << parameter_meta.at(i) <<
                string_builder::to_string;
            VLOG_LP(log_error) << msg;
            return false;
        }
    }
    return true;
}

static void dump_file_metadata(arrow::ipc::RecordBatchFileReader& reader) {
    VLOG_LP(log_debug) << "*** begin dump metadata for arrow file ***";
    // VLOG_LP(log_debug) << "metadata version:" << reader.version();
    // ARROW_ASSIGN_OR_RAISE(auto rows, reader.CountRows());
    // VLOG_LP(log_debug) << "num_rows:" << rows;

    VLOG_LP(log_debug) << "num_record_batches:" << reader.num_record_batches();

    auto& schema = *reader.schema();
    VLOG_LP(log_debug) << "num_columns:" << schema.num_fields();
    for(std::size_t i=0, n=reader.schema()->num_fields(); i<n; ++i) {
        auto&& c = reader.schema()->field(static_cast<int>(i));
        std::stringstream ss{};
        ss << "  column name:" << c->name() << " type:" << *c->type();
        VLOG_LP(log_debug) << ss.str();
    }

    VLOG_LP(log_debug) << "*** end dump metadata for arrow file ***";
}

bool arrow_reader::init(
    std::string_view path,
    reader_option const* opt,
    std::size_t row_group_index
) {
    try {
        path_ = std::string{path};
        {
            auto res = arrow::io::ReadableFile::Open(path_.string(), arrow::default_memory_pool());
            if(! res.ok()) {
                throw_exception(std::domain_error{
                    string_builder{} << "opening Arrow file failed with error: " << res.status() << string_builder::to_string
                });
            }
            input_file_ = res.ValueUnsafe();
        }
        {
            auto res = arrow::ipc::RecordBatchFileReader::Open(input_file_);
            if(! res.ok()) {
                throw_exception(std::domain_error{
                    string_builder{} << "opening Arrow file reader failed with error: " << res.status() << string_builder::to_string
                });
            }
            file_reader_ = res.ValueUnsafe();
        }

        dump_file_metadata(*file_reader_);
        row_group_count_ = file_reader_->num_record_batches();

        if(row_group_index != index_unspecified && row_group_index >= row_group_count_) {
            VLOG_LP(log_error) << "row group index:" << row_group_index <<
                    " too large for row group count:" << row_group_count_;
            return false;
        }
        row_group_index_ = row_group_index == index_unspecified ? 0 : row_group_index;
        if(opt != nullptr) {
            parameter_meta_ = maybe_shared_ptr{opt->meta_};
            if(! validate_option(*opt, *file_reader_->schema())) {
                return false;
            }
            parameter_to_field_ = create_parameter_to_field(*opt, *file_reader_->schema());
            meta_ = create_meta(*file_reader_->schema(), parameter_meta_.get(), std::addressof(parameter_to_field_));
            if(! validate_parameter_mapping(parameter_to_field_, *parameter_meta_, *meta_)) {
                return false;
            }

        } else {
            // this is for testing - create mock option
            meta_ = create_meta(*file_reader_->schema(), nullptr, nullptr);
            reader_option d = create_default(*meta_->origin());
            parameter_meta_ = maybe_shared_ptr{d.meta_};
            parameter_to_field_ = create_parameter_to_field(d, *file_reader_->schema());
        }
        if(! meta_) {
            return false;
        }

        // columns_ = create_columns_meta(*file_metadata);
        buf_ = data::aligned_buffer{parameter_meta_->record_size(), parameter_meta_->record_alignment()};
        buf_.resize(parameter_meta_->record_size());

        {
            auto res = file_reader_->ReadRecordBatch(static_cast<int>(row_group_index_));
            if(! res.ok()) {
                throw_exception(std::domain_error{
                    string_builder{} << "reading from Arrow file reader failed with error: " << res.status()
                                     << string_builder::to_string
                });
            }
            record_batch_ = res.ValueUnsafe();
        }
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Arrow reader init error: " << e.what();
        return false;
    }
    return true;
}

std::size_t arrow_reader::row_group_count() const noexcept {
    return row_group_count_;
}

arrow_reader::~arrow_reader() noexcept {
    close();
}

std::shared_ptr<arrow::RecordBatch> const& arrow_reader::record_batch() const noexcept {
    return record_batch_;
}

}  // namespace jogasaki::executor::file
