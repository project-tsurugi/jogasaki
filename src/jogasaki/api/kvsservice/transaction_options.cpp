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

#include <jogasaki/api/kvsservice/transaction_option.h>

#include <utility>

#include <jogasaki/api/kvsservice/transaction_type.h>

namespace jogasaki::api::kvsservice {

transaction_option::transaction_option(enum transaction_type type, table_areas write_preserves) noexcept :
        type_(type), write_preserves_(std::move(write_preserves)) {
}
}
