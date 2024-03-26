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
#include "group_meta.h"

#include <memory>
#include <utility>

#include <jogasaki/meta/record_meta.h>

namespace jogasaki::meta {

group_meta::group_meta() :
    key_meta_(std::make_shared<meta::record_meta>()),
    value_meta_(std::make_shared<meta::record_meta>())
{}

group_meta::group_meta(
    group_meta::record_meta_type key_meta,
    group_meta::record_meta_type value_meta
) :
    key_meta_(std::move(key_meta)),
    value_meta_(std::move(value_meta))
{}

group_meta::group_meta(
    const record_meta &key_meta,
    const record_meta &value_meta
) :
    group_meta(
        std::make_shared<record_meta>(key_meta),
        std::make_shared<record_meta>(value_meta)
    )
{}

record_meta const& group_meta::key() const noexcept {
    return *key_meta_;
}

group_meta::record_meta_type const& group_meta::key_shared() const noexcept {
    return key_meta_;
}

record_meta const& group_meta::value() const noexcept {
    return *value_meta_;
}

group_meta::record_meta_type const& group_meta::value_shared() const noexcept {
    return value_meta_;
}
} // namespace

