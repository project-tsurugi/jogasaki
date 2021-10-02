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

#include <tateyama/api/server/service.h>

/**
 * @brief C-interface for environment factory
 * @details extern "C" version of create environment. Prefer create_environment when C API is not necessary.
 * This function passes ownership to caller, that must call `delete_environment()` when finish using.
 * @return environment object
 */
extern "C" tateyama::api::server::service* new_application(jogasaki::api::database* db);

/**
 * @brief C-interface to delete environment
 * @param env the environment object to delete
 */
extern "C" void delete_application(tateyama::api::server::service* app);
