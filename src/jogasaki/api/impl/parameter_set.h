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

#include <jogasaki/api/parameter_set.h>
#include <jogasaki/plan/parameter_set.h>
#include <jogasaki/accessor/text.h>

namespace jogasaki::api::impl {

using kind = field_type_kind;

/**
 * @brief parameter_set implementation
 */
class parameter_set : public api::parameter_set {
public:
    parameter_set() = default;

    explicit parameter_set(std::shared_ptr<plan::parameter_set> body) noexcept;

    void set_int4(std::string_view name, field_type_traits<kind::int4>::runtime_type value) override;
    void set_int8(std::string_view name, field_type_traits<kind::int8>::runtime_type value) override;
    void set_float4(std::string_view name, field_type_traits<kind::float4>::runtime_type value) override;
    void set_float8(std::string_view name, field_type_traits<kind::float8>::runtime_type value) override;
    void set_character(std::string_view name, field_type_traits<kind::character>::runtime_type value) override;
    void set_decimal(std::string_view name, field_type_traits<kind::decimal>::runtime_type value) override;
    void set_date(std::string_view name, field_type_traits<kind::date>::runtime_type value) override;
    void set_time_of_day(std::string_view name, field_type_traits<kind::time_of_day>::runtime_type value) override;
    void set_time_point(std::string_view name, field_type_traits<kind::time_point>::runtime_type value) override;

    void set_reference_column(std::string_view name, std::size_t position) override;
    void set_reference_column(std::string_view name, std::string_view column_name) override;

    void set_null(std::string_view name) override;

    [[nodiscard]] parameter_set* clone() const& override;
    [[nodiscard]] parameter_set* clone() && override;

    /**
     * @brief accessor to the wrapped object
     * @return plan::parameter_set implementation
     */
    [[nodiscard]] std::shared_ptr<plan::parameter_set> const& body() const noexcept;
private:
    std::shared_ptr<plan::parameter_set> body_{std::make_shared<plan::parameter_set>()};

};


}
