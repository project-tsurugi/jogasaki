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
#include "parameter_entry.h"

#include <jogasaki/data/value.h>
#include <jogasaki/meta/field_type.h>

namespace jogasaki::plan {

parameter_entry::parameter_entry(meta::field_type type, data::value value) :
    type_(std::move(type)),
    value_(std::move(value))  //NOLINT(hicpp-move-const-arg,performance-move-const-arg)
{}

meta::field_type const& parameter_entry::type() const noexcept {
    return type_;
}

data::value const& parameter_entry::value() const noexcept {
    return value_;
}

any parameter_entry::as_any() const noexcept {
    return value_.view();
}

}
