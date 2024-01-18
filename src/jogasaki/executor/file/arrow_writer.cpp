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
#include "arrow_writer.h"

#include <array>
#include <iomanip>
#include <arrow/array.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/decimal.h>
#include <arrow/util/logging.h>
#include <glog/logging.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/constants.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/utils/decimal.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::string_builder;
using takatori::util::throw_exception;

arrow_writer::arrow_writer(maybe_shared_ptr<meta::external_record_meta> meta) :
    meta_(std::move(meta))
{}

std::shared_ptr<arrow::ArrayBuilder>
create_array_builder(meta::field_type const& type, std::shared_ptr<arrow::DataType> const& arrow_type) {
    using k = meta::field_type_kind;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    switch(type.kind()) {
        case k::int1: return std::make_shared<arrow::Int8Builder>(pool);
        case k::int2: return std::make_shared<arrow::Int16Builder>(pool);
        case k::int4: return std::make_shared<arrow::Int32Builder>(pool);
        case k::int8: return std::make_shared<arrow::Int64Builder>(pool);
        case k::float4: return std::make_shared<arrow::FloatBuilder>(pool);
        case k::float8: return std::make_shared<arrow::DoubleBuilder>(pool);
        case k::character: {
            if(type.option_unsafe<k::character>()->varying_) {
                return std::make_shared<arrow::StringBuilder>(pool);
            }
            return std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow_type, pool);
        }
        case k::octet: return std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow_type, pool); //TODO
        case k::decimal: return std::make_shared<arrow::Decimal128Builder> (arrow_type, pool);
        case k::date: return std::make_shared<arrow::Date32Builder>(pool);
        case k::time_of_day: return std::make_shared<arrow::Time64Builder>(arrow_type, pool);
        case k::time_point: return std::make_shared<arrow::TimestampBuilder>(arrow_type, pool);
        default:
            break;
    }
    std::abort();
}

bool check_status(std::function<::arrow::Status()> const& fn) {
    auto st = fn();
    auto ret = st.ok();
    if(! ret) {
        VLOG_LP(log_error) << "Arrow writer error: " << st;
    }
    return ret;
}

void arrow_writer::finish() {
    arrays_.clear();
    arrays_.reserve(meta_->field_count());
    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        auto& e = arrays_.emplace_back();
        check_status([&](){
            ARROW_RETURN_NOT_OK(array_builders_[i]->Finish(&e));
            return arrow::Status::OK();
        });
    }
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema_, arrays_);
    check_status([&]() {
        ARROW_RETURN_NOT_OK(record_batch_writer_->WriteTable(*table));
        ARROW_RETURN_NOT_OK(record_batch_writer_->Close());
        return arrow::Status::OK();
    });
}

void arrow_writer::new_row_group() {
    if(! array_builders_.empty()) {
        finish();
    }
    array_builders_.clear();
    array_builders_.reserve(meta_->field_count());
    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        array_builders_.emplace_back(create_array_builder(meta_->at(i), schema_->field(static_cast<int>(i))->type()));
    }
}

bool arrow_writer::init(std::string_view path) {
    try {
        path_ = std::string{path};
        check_status([&]() {
            ARROW_ASSIGN_OR_RAISE(fs_ , ::arrow::io::FileOutputStream::Open(path_.string()));
            return ::arrow::Status::OK();
        });
        auto [schema, colopts] = create_schema();
        schema_ = schema;
        column_options_ = std::move(colopts);

        arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();
        // FIXME fill options
        check_status([&]() {
            ARROW_ASSIGN_OR_RAISE(record_batch_writer_, ::arrow::ipc::MakeFileWriter(fs_, schema_, options));
            return ::arrow::Status::OK();
        });
        new_row_group();
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Arrow writer init error: " << e.what();
        return false;
    }
    return true;
}

bool arrow_writer::write(accessor::record_ref ref) {
    try {
        using k = meta::field_type_kind;
        for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
            bool null = ref.is_null(meta_->nullity_offset(i)) && meta_->nullable(i);
            if(null) {
                (void) array_builders_[i]->AppendNull();
                continue;
            }
            bool success{false};
            switch(meta_->at(i).kind()) {
                case k::int1: success = write_int1(i, ref.get_value<std::int32_t>(meta_->value_offset(i))); break;
                case k::int2: success = write_int2(i, ref.get_value<std::int32_t>(meta_->value_offset(i))); break;
                case k::int4: success = write_int4(i, ref.get_value<std::int32_t>(meta_->value_offset(i))); break;
                case k::int8: success = write_int8(i, ref.get_value<std::int64_t>(meta_->value_offset(i))); break;
                case k::float4: success = write_float4(i, ref.get_value<float>(meta_->value_offset(i))); break;
                case k::float8: success = write_float8(i, ref.get_value<double>(meta_->value_offset(i))); break;
                case k::character: success = write_character(i, ref.get_value<accessor::text>(meta_->value_offset(i)), column_options_[i]); break;
                case k::decimal: success = write_decimal(i, ref.get_value<runtime_t<meta::field_type_kind::decimal>>(meta_->value_offset(i)), column_options_[i]); break;
                case k::date: success = write_date(i, ref.get_value<runtime_t<meta::field_type_kind::date>>(meta_->value_offset(i))); break;
                case k::time_of_day: success = write_time_of_day(i, ref.get_value<runtime_t<meta::field_type_kind::time_of_day>>(meta_->value_offset(i))); break;
                case k::time_point: success = write_time_point(i, ref.get_value<runtime_t<meta::field_type_kind::time_point>>(meta_->value_offset(i))); break;
                default:
                    break;
            }
            if(! success) {
                return false;
            }
        }
    } catch (std::exception const& e) {
        VLOG_LP(log_error) << "Arrow writer write error: " << e.what();
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

bool arrow_writer::write_int1(std::size_t colidx, int32_t v) {
    auto& builder = static_cast<arrow::Int8Builder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(static_cast<std::int8_t>(v));
    return true;
}

bool arrow_writer::write_int2(std::size_t colidx, int32_t v) {
    auto& builder = static_cast<arrow::Int16Builder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(static_cast<std::int16_t>(v));
    return true;
}

bool arrow_writer::write_int4(std::size_t colidx, int32_t v) {
    auto& builder = static_cast<arrow::Int32Builder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(v);
    return true;
}

bool arrow_writer::write_int8(std::size_t colidx, std::int64_t v) {
    auto& builder = static_cast<arrow::Int64Builder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(v);
    return true;
}

bool arrow_writer::write_float4(std::size_t colidx, float v) {
    auto& builder = static_cast<arrow::FloatBuilder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(v);
    return true;
}

bool arrow_writer::write_float8(std::size_t colidx, double v) {
    auto& builder = static_cast<arrow::DoubleBuilder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(v);
    return true;
}

bool arrow_writer::write_character(std::size_t colidx, accessor::text v, details::column_option const& colopt) {
    if(colopt.varying_) {
        auto& builder = static_cast<arrow::StringBuilder&>(*array_builders_[colidx]);  //NOLINT
        auto sv = static_cast<std::string_view>(v);
        (void) builder.Append(sv.data(), static_cast<int>(sv.size()));
        return true;
    }
    auto& builder = static_cast<arrow::FixedSizeBinaryBuilder&>(*array_builders_[colidx]);  //NOLINT
    auto sv = static_cast<std::string_view>(v);
    // arrow assumes the buffer has enough length, so check length first
    if(sv.size() != colopt.length_) {
        throw_exception(std::logic_error{
            string_builder{} << "invalid length(" << sv.size() << ") for character field with length "
                             << colopt.length_ << string_builder::to_string
        });
    }
    (void) builder.Append(sv.data());
    return true;
}

bool arrow_writer::write_decimal(
    std::size_t colidx,
    runtime_t<meta::field_type_kind::decimal> v,
    details::column_option const& colopt
) {
    (void) colopt;
    auto& builder = static_cast<arrow::Decimal128Builder&>(*array_builders_[colidx]);  //NOLINT
    arrow::BasicDecimal128 d{arrow::Decimal128::WordArray{v.coefficient_low(), v.coefficient_high()}};
    if(v.sign() < 0) {
        d.Negate();
    }
    (void) builder.Append(d);
    return true;
}

bool arrow_writer::write_date(std::size_t colidx, runtime_t<meta::field_type_kind::date> v) {
    auto d = static_cast<std::int32_t>(v.days_since_epoch());
    auto& builder = static_cast<arrow::Date32Builder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(d);
    return true;
}

bool arrow_writer::write_time_of_day(std::size_t colidx, runtime_t<meta::field_type_kind::time_of_day> v) {
    auto ns = static_cast<std::int64_t>(v.time_since_epoch().count());
    auto& builder = static_cast<arrow::Time64Builder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(ns);
    return true;
}

bool arrow_writer::write_time_point(std::size_t colidx, runtime_t<meta::field_type_kind::time_point> v) {
    auto secs = static_cast<std::int64_t>(v.seconds_since_epoch().count());
    auto subsecs = static_cast<std::int64_t>(v.subsecond().count());
    auto& builder = static_cast<arrow::TimestampBuilder&>(*array_builders_[colidx]);  //NOLINT
    (void) builder.Append(secs*1000*1000*1000 + subsecs);
    return true;
}

bool arrow_writer::close() {
    if(! array_builders_.empty()) {
        try {
            finish();
            // Write the bytes to file
            DCHECK(fs_->Close().ok());  // do we need this? sample doesn't close output stream
        } catch (std::exception const& e) {
            VLOG_LP(log_error) << "Arrow writer close error: " << e.what();
            return false;
        }
        array_builders_.clear();
    }
    return true;
}

std::shared_ptr<arrow_writer>
arrow_writer::open(maybe_shared_ptr<meta::external_record_meta> meta, std::string_view path) {
    auto ret = std::make_shared<arrow_writer>(std::move(meta));
    if(ret->init(path)) {
        return ret;
    }
    return {};
}

std::pair<std::shared_ptr<arrow::Schema>, std::vector<details::column_option>> arrow_writer::create_schema() {
    std::vector<std::shared_ptr<arrow::Field>> fields{};
    fields.reserve(meta_->field_count());
    std::vector<details::column_option> options{};
    options.resize(meta_->field_count());

    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        std::string name{};
        if(auto o = meta_->field_name(i)) {
            name = o.value();
        }
        std::shared_ptr<arrow::DataType> type{};
        switch(meta_->at(i).kind()) {
            case meta::field_type_kind::int1: {
                type = arrow::int8();
                break;
            }
            case meta::field_type_kind::int2: {
                type = arrow::int16();
                break;
            }
            case meta::field_type_kind::int4: {
                type = arrow::int32();
                break;
            }
            case meta::field_type_kind::int8: {
                type = arrow::int64();
                break;
            }
            case meta::field_type_kind::float4: {
                type = arrow::float32();
                break;
            }
            case meta::field_type_kind::float8: {
                type = arrow::float64();
                break;
            }
            case meta::field_type_kind::character: {
                auto opt = meta_->at(i).option<meta::field_type_kind::character>();
                options[i].varying_ = opt->varying_;
                options[i].length_ =
                    opt->length_.has_value() ? opt->length_.value() : details::column_option::undefined;
                if(opt->varying_) {
                    type = arrow::utf8();
                    break;
                }
                if(! opt->length_.has_value()) {
                    throw_exception(std::logic_error{"no length for char field"});
                }
                type = arrow::fixed_size_binary(static_cast<std::int32_t>(*opt->length_));
                break;
            }
            case meta::field_type_kind::octet: {
                // TODO get length and varying flag to distinguish BINARY/VARBINARY
                type = arrow::fixed_size_binary(10);
                break;
            }
            case meta::field_type_kind::decimal: {
                auto opt = meta_->at(i).option<meta::field_type_kind::decimal>();
                std::size_t p = opt->precision_.has_value() ? *opt->precision_ : decimal_default_precision;
                std::size_t s = opt->scale_.has_value() ? *opt->scale_: dumped_decimal_default_scale;
                type = arrow::decimal128(static_cast<std::int32_t>(p), static_cast<std::int32_t>(s));
                options[i].precision_ = p;
                options[i].scale_ = s;
                break;
            }
            case meta::field_type_kind::date: {
                type = arrow::date32();
                break;
            }
            case meta::field_type_kind::time_of_day: {
                auto opt = meta_->at(i).option<meta::field_type_kind::time_of_day>();
                (void) opt->with_offset_;
                // TODO handle offset
                type = arrow::time64(arrow::TimeUnit::type::NANO);
                break;
            }
            case meta::field_type_kind::time_point: {
                auto opt = meta_->at(i).option<meta::field_type_kind::time_point>();
                auto tz = opt->with_offset_ ? "UTC" : "";
                type = arrow::timestamp(arrow::TimeUnit::type::NANO, tz);
                break;
            }
            default:
                break;
        }
        fields.emplace_back(arrow::field(name, type));
    }
    return {
        std::make_shared<arrow::Schema>(std::move(fields)),
        std::move(options)
    };
}

std::string arrow_writer::path() const noexcept {
    return path_.string();
}

std::size_t arrow_writer::write_count() const noexcept {
    return write_count_;
}

arrow_writer::~arrow_writer() noexcept {
    close();
}

}  // namespace jogasaki::executor::file
