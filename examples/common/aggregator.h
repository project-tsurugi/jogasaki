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

#include <functional>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::common_cli {

using aggregator_type = std::function<void (meta::record_meta const*, accessor::record_ref, accessor::record_ref)>;

using key_type = std::int64_t;
using value_type = double;

struct access {
    value_type get_value(accessor::record_ref value) {
        return value.get_value<value_type>(value_meta_->value_offset(0));
    }
    void set_value(accessor::record_ref value, value_type arg) {
        value.set_value<value_type>(value_meta_->value_offset(0), arg);
    }
    meta::record_meta const* value_meta_{};
};

std::shared_ptr<aggregator_type> create_aggregator() {
    return std::make_shared<aggregator_type>([](meta::record_meta const* meta, accessor::record_ref target, accessor::record_ref source){
        access acc{meta};
        auto new_value = acc.get_value(target) + acc.get_value(source);
        acc.set_value(target, new_value);
    });
}

} //namespace

