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
#include "create_req_info.h"

#include <memory>

#include <tateyama/api/server/mock/request_response.h>

namespace jogasaki::utils {

request_info create_req_info(std::string_view username) {
    auto req = std::make_shared<tateyama::api::server::mock::test_request>();
    req->session_info_.username_ = username;
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto info = request_info(0, std::move(req), std::move(res));
    return info;
}

} // namespace jogasaki::utils
