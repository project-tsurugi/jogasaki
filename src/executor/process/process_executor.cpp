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
#include "process_executor.h"

namespace jogasaki::executor::process {

void processor_context::initialize() {}

processor_context::readers_list processor_context::readers() { return {}; }

processor_context::readers_list processor_context::subinput_readers() { return {}; }

processor_context::writers_list processor_context::writers() { return {}; }

void processor::initialize(processor_context& context) { (void)context; }

process_executor::process_executor() {}


}


