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

#include <memory>

#include "datastore.h"

#include <jogasaki/kvs/database.h>

namespace jogasaki::datastore {

/**
* @brief get mock or production datastore instance
* @param db database to fetch the production datastore. You can pass nullptr
* if only mock instance is necessary.
* @param reset_cache indicates whether to clear and reset the cached object. Use for testing.
* @return the datastore instance
*/
datastore* get_datastore(kvs::database* db, bool reset_cache = false);

}  // namespace jogasaki::datastore
