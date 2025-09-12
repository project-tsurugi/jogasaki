/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <jogasaki/executor/dto/common_column.h>
#include <jogasaki/meta/external_record_meta.h>

namespace jogasaki::executor {

/**
 * @brief convert to dto common columns from record meta
 * @param meta the source record meta
 * @return the list of dto columns
 */
std::vector<dto::common_column> to_common_columns(meta::external_record_meta const& meta);

}  // namespace jogasaki::executor

