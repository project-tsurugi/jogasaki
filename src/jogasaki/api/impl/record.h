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
#pragma once

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/record.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/accessor/record_printer.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;

/**
 * @brief Record object in the result set
 */
class record : public api::record {
public:
    using kind = api::field_type_kind;

    template<kind Kind>
    using runtime_type = typename api::field_type_traits<Kind>::runtime_type;

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
    );

    explicit record(maybe_shared_ptr<meta::record_meta> meta);

    using k = meta::field_type_kind;

    [[nodiscard]] runtime_type<kind::int4> get_int4(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::int8> get_int8(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::float4> get_float4(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::float8> get_float8(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::character> get_character(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::octet> get_octet(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::decimal> get_decimal(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::date> get_date(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::time_of_day> get_time_of_day(std::size_t index) const override;
    [[nodiscard]] runtime_type<kind::time_point> get_time_point(std::size_t index) const override;

    [[nodiscard]] bool is_null(size_t index) const noexcept override;

    void ref(accessor::record_ref r) noexcept;

    [[nodiscard]] accessor::record_ref ref() const noexcept;

    void write_to(std::ostream& os) const noexcept override;

private:
    accessor::record_ref ref_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
};

}

