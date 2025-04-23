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

#include <cstdint>

#include <jogasaki/transaction_context.h>

namespace jogasaki::utils {

/**
 * @brief check if parallel scan is enabled by global setting or for the given transaction context.
 * @param tctx The transaction context to check.
 * @return A pair containing a boolean indicating if parallel scan is enabled and the number of parallelism for scan.
 */
std::pair<bool, std::uint32_t> scan_parallel_enabled(transaction_context const& tctx);

} // namespace jogasaki::utils
