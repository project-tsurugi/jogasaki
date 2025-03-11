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
#include "assign_lob_id.h"

#include <jogasaki/datastore/register_lob.h>

namespace jogasaki::datastore {

/**
 * @brief register lob reference and publish new id if needed
 * @param ref the input lob reference to register
 * @param tx transaction to keep the scope object (blob pool) for the lob data
 * @param id [out] blob id assigned for the input lob data
 * @param error [out] error information is set when status code other than status::ok is returned
 * @return status::ok when successful
 * @return status::err_io_error when datastore met io error
 * @return any other error otherwise
 */
status assign_lob_id(
    lob::lob_reference const& ref,
    transaction_context* tx,
    lob::lob_id_type& id,
    std::shared_ptr<error::error_info>& error
) {
    using k = lob::lob_reference_kind;
    switch (ref.kind()) {
        case k::provided: {
            if (auto res = register_lob(ref.locator()->path(), ref.locator()->is_temporary(), tx, id, error); res != status::ok) {
                return res;
            }
            break;
        }
        case k::fetched: {
            if (auto res = duplicate_lob(ref.object_id(), tx, id, error); res != status::ok) {
                return res;
            }
        }
        case k::undefined:
        case k::resolved:
            // no-op
            break;
    }
    return status::ok;
}

}  // namespace jogasaki::datastore
