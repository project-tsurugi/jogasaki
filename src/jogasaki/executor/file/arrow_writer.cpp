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

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <type_traits>
#include <variant>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/file.h>
#include <arrow/ipc/options.h>
#include <arrow/ipc/type_fwd.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/basic_decimal.h>
#include <arrow/util/decimal.h>
#include <arrow/util/string_builder.h>
#include <glog/logging.h>

#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/file/column_option.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::string_builder;
using takatori::util::throw_exception;

arrow_writer::arrow_writer(maybe_shared_ptr<meta::external_record_meta> meta, arrow_writer_option opt) :
    meta_(std::move(meta)),
    option_(std::move(opt))
{}

std::shared_ptr<arrow::ArrayBuilder> create_array_builder(
    meta::field_type const& type,
    std::shared_ptr<arrow::DataType> const& arrow_type,
    arrow_writer_option const& opts
) {
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
            if(type.option_unsafe<k::character>()->varying_ || ! opts.use_fixed_size_binary_for_char()) {
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

/**
 * @throw std::domain_error if arrow returns error
*/
void arrow_writer::finish() {
    arrays_.clear();
    arrays_.reserve(meta_->field_count());
    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        auto& e = arrays_.emplace_back();
        auto st = array_builders_[i]->Finish(&e);
        if(! st.ok()) {
            throw_exception(std::domain_error{
                string_builder{} << "finishing Arrow file failed with error: " << st << string_builder::to_string
            });
        }
    }
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema_, arrays_);
    auto st = record_batch_writer_->WriteTable(*table);
    if(! st.ok()) {
        throw_exception(std::domain_error{
            string_builder{} << "writing Arrow table failed with error: " << st << string_builder::to_string
        });
    }
}

void arrow_writer::new_row_group() {
    if(! array_builders_.empty()) {
        finish();
    }
    array_builders_.clear();
    array_builders_.reserve(meta_->field_count());
    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        array_builders_.emplace_back(
            create_array_builder(meta_->at(i), schema_->field(static_cast<int>(i))->type(), option_)
        );
    }
    row_group_write_count_ = 0;
}

arrow::ipc::IpcWriteOptions create_options(arrow_writer_option const& in) {
    arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();

    if(in.metadata_version() == "V1") {
        options.metadata_version = arrow::ipc::MetadataVersion::V1;
    } else if(in.metadata_version() == "V2") {
        options.metadata_version = arrow::ipc::MetadataVersion::V2;
    } else if(in.metadata_version() == "V3") {
        options.metadata_version = arrow::ipc::MetadataVersion::V3;
    } else if(in.metadata_version() == "V4") {
        options.metadata_version = arrow::ipc::MetadataVersion::V4;
    } else if(in.metadata_version() == "V5") {
        options.metadata_version = arrow::ipc::MetadataVersion::V5;
    } else {
        throw_exception(std::domain_error {
            string_builder{} << "invalid value '" << in.metadata_version() << "' for option metadata_version"
                             << string_builder::to_string
        });
    }

    options.alignment = in.alignment();
    if(in.min_space_saving() != 0) {
        options.min_space_savings = in.min_space_saving();
    }
    // TODO validate and pass codec
    return options;
}

std::size_t arrow_writer::estimate_avg_record_size() {
    std::size_t sz{};
    for(std::size_t i=0, n=meta_->field_count(); i<n; ++i) {
        using k = meta::field_type_kind;
        switch(meta_->at(i).kind()) {
            case k::int1: sz += 1; break;
            case k::int2: sz += 2; break;
            case k::int4: sz += 4; break;
            case k::int8: sz += 8; break;
            case k::float4: sz += 4; break;
            case k::float8: sz += 8; break;
            case k::character: {
                std::size_t len{100};  // assuming default max len for varchar(*)/char(*)
                if(column_options_[i].length_ != details::column_option::undefined) {
                    len = column_options_[i].length_;
                }
                if(column_options_[i].varying_) {
                    sz += len / 2;
                } else {
                    sz += len;
                }
                break;
            }
            case k::decimal: sz += 16; break;
            case k::date: sz += 4; break;
            case k::time_of_day: sz += 8; break;
            case k::time_point: sz += 8; break;
            default:
                break;
        }
    }
    return sz;
}

void arrow_writer::calculate_batch_size() {
    constexpr static std::size_t default_batch_in_bytes = 64UL * 1024UL * 1024UL;
    std::size_t avg_record_sz = estimate_avg_record_size();
    std::size_t size_from_bytes = option_.record_batch_in_bytes() / avg_record_sz;
    std::size_t default_size_from_bytes = default_batch_in_bytes / avg_record_sz;
    std::size_t size = option_.record_batch_size();
    if(size_from_bytes == 0 && size == 0) {
        calculated_batch_size_ = default_size_from_bytes;
        return;
    }
    if(size == 0) {
        calculated_batch_size_ = size_from_bytes;
        return;
    }
    if(size_from_bytes == 0) {
        calculated_batch_size_ = size;
        return;
    }
    calculated_batch_size_ = std::min(size_from_bytes, size);
}

std::size_t arrow_writer::row_group_max_records() const noexcept {
    return calculated_batch_size_;
}

bool arrow_writer::init(std::string_view path) {
    try {
        path_ = std::string{path};
        {
            auto res = ::arrow::io::FileOutputStream::Open(path_.string());
            if(! res.ok()) {
                throw_exception(std::domain_error{
                    string_builder{} << "opening Arrow file failed with error: " << res.status()
                                     << string_builder::to_string
                });
            }
            fs_ = res.ValueUnsafe();
        }
        auto [schema, colopts] = create_schema();
        schema_ = schema;
        column_options_ = std::move(colopts);

        auto options = create_options(option_);

        {
            auto res = ::arrow::ipc::MakeFileWriter(fs_, schema_, options);
            if(! res.ok()) {
                throw_exception(std::domain_error{
                    string_builder{} << "creating Arrow file writer failed with error: " << res.status()
                                     << string_builder::to_string
                });
            }
            record_batch_writer_ = res.ValueUnsafe();
        }

        calculate_batch_size();
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
        if(row_group_write_count_ >= calculated_batch_size_) {
            new_row_group();
        }
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
    ++row_group_write_count_;
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
    if(colopt.varying_ || ! option_.use_fixed_size_binary_for_char()) {
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
            auto res = record_batch_writer_->Close();
            if(! res.ok()) {
                VLOG_LP(log_error) << "Arrow writer close error:" << res;
                return false;
            }
            (void) res;
            // Write the bytes to file
            DCHECK(fs_->Close().ok());
        } catch (std::exception const& e) {
            VLOG_LP(log_error) << "Arrow writer close error: " << e.what();
            return false;
        }
        array_builders_.clear();
    }
    return true;
}

std::shared_ptr<arrow_writer> arrow_writer::open(
    maybe_shared_ptr<meta::external_record_meta> meta,
    std::string_view path,
    arrow_writer_option opt
) {
    auto ret = std::make_shared<arrow_writer>(std::move(meta), std::move(opt));
    if(ret->init(path)) {
        return ret;
    }
    return {};
}

std::pair<std::shared_ptr<arrow::Schema>, std::vector<details::column_option>> arrow_writer::create_schema() {  //NOLINT(readability-function-cognitive-complexity)
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
                if(opt->varying_ || ! option_.use_fixed_size_binary_for_char()) {
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
                auto opt = meta_->at(i).option<meta::field_type_kind::octet>();
                options[i].varying_ = opt->varying_;
                options[i].length_ =
                    opt->length_.has_value() ? opt->length_.value() : details::column_option::undefined;
                if(opt->varying_) {
                    type = arrow::binary();
                    break;
                }
                if(! opt->length_.has_value()) {
                    throw_exception(std::logic_error{"no length for binary field"});
                }
                type = arrow::fixed_size_binary(static_cast<std::int32_t>(*opt->length_));
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

std::size_t arrow_writer::calculated_batch_size() const noexcept {
    return calculated_batch_size_;
}

}  // namespace jogasaki::executor::file
