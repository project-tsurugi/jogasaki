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
#include <jogasaki/relay/blob_session_container.h>

#include <data-relay-grpc/blob_relay/service.h>
#include <data-relay-grpc/blob_relay/session.h>

#include <jogasaki/executor/global.h>

namespace jogasaki::relay {

template<>
data_relay_grpc::blob_relay::blob_session* basic_blob_session_container<data_relay_grpc::blob_relay::blob_session>::get_or_create() {
    if (! session_) {
        auto relay_service = global::relay_service();
        if (relay_service) {
            auto& session = relay_service->create_session(transaction_id_);
            session_ = std::addressof(session);
        }
    }
    return session_;
}

}
