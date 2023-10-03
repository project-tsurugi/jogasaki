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
#include "data_channel.h"

#include <string_view>
#include <memory>

#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>

#include <tateyama/status.h>
#include <tateyama/api/server/data_channel.h>

#include <jogasaki/api/impl/data_writer.h>

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
using takatori::util::fail;

data_channel::data_channel(std::shared_ptr<tateyama::api::server::data_channel> origin) :
    origin_(std::move(origin))
{}

status data_channel::acquire(std::shared_ptr<writer>& wrt) {
    std::shared_ptr<tateyama::api::server::writer> writer{};
    if(auto rc = origin_->acquire(writer); rc != tateyama::status::ok) {
        fail();
    }
    wrt = std::make_shared<data_writer>(std::move(writer));
    return status::ok;
}

status data_channel::release(writer& wrt) {
    auto& w = unsafe_downcast<data_writer>(wrt);
    if(auto rc = origin_->release(*w.origin()); rc != tateyama::status::ok) {
        fail();
    }
    return status::ok;
}

std::shared_ptr<tateyama::api::server::data_channel> const& data_channel::origin() const noexcept {
    return origin_;
}
}

