/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/processor_info.h>

namespace jogasaki::executor::process {

/**
 * @brief returns whether the process must not be split into multiple partitions
 * @details the process handling an ordered relation must be processed by single partition, otherwise the order is
 * lost (tsurugi-issues#1589 R4). Such process is the one writing to the forward exchange with respect_input or
 * preserve_output, reading from the forward exchange with preserve_output, or emitting the records with
 * preserve_output.
 * @param info the processor information
 * @param exchanges the input/output exchanges of the process
 * @return true if the process must be processed by single partition
 * @return false otherwise
 */
[[nodiscard]] bool requires_single_partition(processor_info const& info, io_exchange_map const& exchanges);

}  // namespace jogasaki::executor::process
