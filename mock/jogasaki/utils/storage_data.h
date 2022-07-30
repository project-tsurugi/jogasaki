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
#include <takatori/type/character.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/logging.h>
#include <jogasaki/data/any.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/utils/random.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/storage_dump.h>
#include <jogasaki/error.h>
#include <jogasaki/api.h>
#include <jogasaki/utils/create_tx.h>

namespace jogasaki::utils {

using namespace std::string_literals;
using namespace std::string_view_literals;
using kind = meta::field_type_kind;
using takatori::util::fail;
using yugawara::storage::configurable_provider;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::expression;

using any = data::any;

void populate_storage_data(
    kvs::database* db,
    std::shared_ptr<configurable_provider> const& provider,
    std::string_view storage_name,
    std::size_t records_per_partition,
    bool sequential_data,
    std::size_t modulo = -1
);

void load_storage_data(
    api::database& db,
    std::shared_ptr<configurable_provider> const& provider,
    std::string_view table_name,
    std::size_t records_per_partition,
    bool sequential_data,
    std::size_t modulo = -1
);

} //namespace jogasaki::utils
