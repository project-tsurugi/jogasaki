/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <cstddef>
#include <vector>

#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/index.h>

#include <jogasaki/error.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/logging.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::index {

using takatori::util::maybe_shared_ptr;

/**
 * @brief create meta for the variables' store for index key/value
 * @param idx yugawara index whose key/value are used to create the meta
 * @param for_key specify whether to create meta for key or not
 * @return record meta for the key or value store
 */
maybe_shared_ptr<meta::record_meta> create_meta(yugawara::storage::index const& idx, bool for_key);

}
