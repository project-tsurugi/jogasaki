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
#pragma once

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_printer.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;

/**
 * @brief Record object in the result set
 */
class record : public api::record {
public:
    /**
     * @brief construct
     */
    record() = default;

    /**
     * @brief copy construct
     */
    record(record const&) = default;

    /**
     * @brief move construct
     */
    record(record &&) = default;

    /**
     * @brief copy assign
     */
    record& operator=(record const&) = default;

    /**
     * @brief move assign
     */
    record& operator=(record &&) = default;

    /**
     * @brief destruct record
     */
    ~record() override = default;

    record(
        accessor::record_ref ref,
        maybe_shared_ptr<meta::record_meta> meta
    ) :
        ref_(ref),
        meta_(std::move(meta))
    {}

    explicit record(
        maybe_shared_ptr<meta::record_meta> meta
    ) :
        record({}, std::move(meta))
    {}

    using k = meta::field_type_kind;
    field_type_traits<kind::int4>::runtime_type get_int4(std::size_t index) const override {
        return ref_.get_value<meta::field_type_traits<k::int4>::runtime_type>(meta_->value_offset(index));
    }
    field_type_traits<kind::int8>::runtime_type get_int8(std::size_t index) const override {
        return ref_.get_value<meta::field_type_traits<k::int8>::runtime_type>(meta_->value_offset(index));
    }
    field_type_traits<kind::float4>::runtime_type get_float4(std::size_t index) const override {
        return ref_.get_value<meta::field_type_traits<k::float4>::runtime_type>(meta_->value_offset(index));
    }
    field_type_traits<kind::float8>::runtime_type get_float8(std::size_t index) const override {
        return ref_.get_value<meta::field_type_traits<k::float8>::runtime_type>(meta_->value_offset(index));
    }
    field_type_traits<kind::character>::runtime_type get_character(std::size_t index) const override {
        return static_cast<field_type_traits<kind::character>::runtime_type>(
            ref_.get_value<meta::field_type_traits<k::character>::runtime_type>(meta_->value_offset(index))
        );
    }

    [[nodiscard]] bool is_null(size_t index) const noexcept override {
        return ref_.is_null(meta_->nullity_offset(index));
    }
    void ref(accessor::record_ref r) noexcept {
        ref_ = r;
    }

    void write_to(std::ostream& os) const noexcept override {
        os << ref_ << *meta_;
    }

private:
    accessor::record_ref ref_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
};

}

