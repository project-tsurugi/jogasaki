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
#include "datastore.h"

#include <sharksfin/api.h>

#include <jogasaki/configuration.h>
#include <jogasaki/datastore/datastore_mock.h>
#include <jogasaki/datastore/datastore_prod.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

namespace jogasaki::datastore {

datastore* get_datastore(bool reset_cache) {
    static std::unique_ptr<datastore> ds = nullptr;
    if (ds && ! reset_cache) {
        return ds.get();
    }
    if(! global::config_pool()->mock_datastore()) {
        std::any a{};
        if(auto res = global::db()->get_datastore(a); res != status::ok) {
            LOG_LP(ERROR) << res << " failed to initialize datastore - falling back to mock";
        } else {
            ds = std::make_unique<datastore_prod>(static_cast<limestone::api::datastore*>(std::any_cast<void*>(a)));
            return ds.get();
        }
    }
    ds = std::make_unique<datastore_mock>();
    return ds.get();
}

}  // namespace jogasaki::datastore
