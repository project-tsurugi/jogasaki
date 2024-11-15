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

#include <memory>

#include <takatori/relation/endpoint_kind.h>

#include <jogasaki/kvs/storage.h>

namespace jogasaki::utils {

namespace relation = takatori::relation;

/**
 * @brief convert takatori::relation::join_kind to jogasaki::kvs::join_kind
 * @param kind the target kind
 * @return the corresponded kind
 * @note this function is public for testing
 */
kvs::end_point_kind from(relation::endpoint_kind type);

}  // namespace jogasaki::utils
