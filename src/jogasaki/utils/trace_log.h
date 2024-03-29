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

#include <glog/logging.h>

#include <jogasaki/logging.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/logging_helper.h>

#define log_entry DVLOG_LP(log_trace) << std::boolalpha << "--> "  //NOLINT
#define log_exit DVLOG_LP(log_trace) << std::boolalpha << "<-- "  //NOLINT
#define binstring(data, len) #data "(len=" << (len) << "):\"" << utils::binary_printer((data), (len)) << "\" " //NOLINT
