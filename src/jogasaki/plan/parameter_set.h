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

#include <shakujo/model/program/Program.h>

namespace jogasaki::plan {

/**
 * @brief parameter for place holders
 */
class parameter_set {
public:
    parameter_set() = default;
    ~parameter_set() = default;
    parameter_set(parameter_set const& other) = default;
    parameter_set(parameter_set&& other) = default;
    parameter_set& operator=(parameter_set const& other) = default;
    parameter_set& operator=(parameter_set&& other) = default;


//    explicit parameter_set(
//        std::map<std::string, std::unique_ptr<shakujo::model::expression::Expression>> values
//    ) :
//        values_(std::move(values))
//    {}

    void set_null(std::string_view name);
    void set_int(std::string_view name, std::int32_t value){
        set_int4(name, value);
    }
    void set_int4(std::string_view name, std::int32_t value);
    void set_int8(std::string_view name, std::int64_t value);
    void set_float8(std::string_view name, double value);
    void set_float4(std::string_view name, float value);
    void set_text(std::string_view name, std::string_view value);
//    std::map<std::string, std::unique_ptr<shakujo::model::expression::Expression>> const& values() const& {
//        return values_;
//    }
//    std::map<std::string, std::unique_ptr<shakujo::model::expression::Expression>>&& values() && {
//        return std::move(values_);
//    }
private:
//    std::map<std::string, std::unique_ptr<shakujo::model::expression::Expression>> values_{};
};

}
