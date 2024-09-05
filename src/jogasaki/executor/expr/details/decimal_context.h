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

#include <cstdint>
#include <decimal.hh>

namespace jogasaki::executor::expr::details {

/**
 * @brief provide standard decimal context for production
 * @details this is a factory function to decimal context commonly used in jogasaki.
 * This function choose deciman context with enough properties for decimal processing (e.g. precision, exponent range, etc.)
*/
decimal::Context standard_decimal_context();

/**
 * @brief ensure decimal context set for the current thread
 * @details mpdecimal depends on thread local storage to keep the decimal context and this function is to ensure
 * the standard context (provided by standard_decimal_context()) is set at least once for the current thread. After the
 * first call on a thread this function is no-op when called again from the thread.
 * @note this function is thread safe and should be called at least once from any thread that uses decimal::Decimal
 * @note mpdecimal has the mechanism to initialize decimal::context with the template set by decimal::context_template.
 * But as far as we tested, it didn't work reliably enough and decimal::context sometime failed to have correct properties,
 * even if we set decimal::context_template before the first use of decimal::context (e.g. global constructor).
 * So we created this function.
*/
void ensure_decimal_context();

/**
 * @brief fetch and reset the status of decimal context for current thread
*/
std::uint32_t reset_decimal_status();

}  // namespace jogasaki::executor::expr::details
