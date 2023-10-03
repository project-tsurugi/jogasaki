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

#include <sharksfin/StatusCode.h>

#include <jogasaki/status.h>

namespace jogasaki::kvs {

/**
 * @brief resolve sharksfin status code to jogasaki status
 * @return resolved status code
 * @note this is generic error mapping and is not applicable to all error situation. Depending on the
 * function requirement, it should manually map the error code.
 */
[[nodiscard]] status resolve(sharksfin::StatusCode code) noexcept;

}

