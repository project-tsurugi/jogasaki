/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <string>
#include <string_view>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::dist {
[[nodiscard]] bool remote_output_supported(meta::record_meta const& meta) noexcept;
[[nodiscard]] std::string encode_remote_record(accessor::record_ref record, meta::record_meta const& meta);
// Variable-length fields borrow payload until the downstream writer copies the record.
[[nodiscard]] bool decode_remote_record(std::string_view payload, meta::record_meta const& meta, accessor::record_ref record, std::string& error);
}
