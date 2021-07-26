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

#include <fstream>
#include <glog/logging.h>
#include <boost/filesystem.hpp>

#include <takatori/util/fail.h>

#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/utils/random.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/storage_dump.h>

namespace jogasaki::common_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using kind = meta::field_type_kind;
using takatori::util::fail;
using yugawara::storage::configurable_provider;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::expression;

constexpr kvs::order asc = kvs::order::ascending;
constexpr kvs::order desc = kvs::order::descending;
constexpr kvs::order undef = kvs::order::undefined;

void dump_storage(std::string_view dir, kvs::database* db, std::string_view storage_name) {
    boost::filesystem::path path{boost::filesystem::current_path()};
    path /= std::string(dir);
    if (! boost::filesystem::exists(path)) {
        if (! boost::filesystem::create_directories(path)) {
            LOG(ERROR) << "creating directory failed";
        }
    }
    std::string file(storage_name);
    file += ".dat";
    path /= file;
    boost::filesystem::ofstream out{path};
    kvs::storage_dump dumper{*db};
    dumper.dump(out, storage_name, 10000);
}

void load_storage(std::string_view dir, kvs::database* db, std::string_view storage_name) {
    boost::filesystem::path path{boost::filesystem::current_path()};
    path /= std::string(dir);
    std::string file(storage_name);
    file += ".dat";
    path /= file;
    if (! boost::filesystem::exists(path)) {
        LOG(ERROR) << "File not found: " << path.string();
        fail();
    }
    boost::filesystem::ifstream in{path};
    kvs::storage_dump dumper{*db};
    dumper.load(in, storage_name, 10000);
}

} //namespace
