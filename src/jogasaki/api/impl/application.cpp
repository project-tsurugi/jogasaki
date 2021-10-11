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
#include <jogasaki/api/application.h>

#include <memory>

#include <jogasaki/api/impl/service.h>

extern "C" tateyama::api::server::service* new_application(jogasaki::api::database* db) {
    return new jogasaki::api::impl::service(*db);
}

extern "C" void delete_application(tateyama::api::server::service* app) {
    delete app;  //NOLINT
}