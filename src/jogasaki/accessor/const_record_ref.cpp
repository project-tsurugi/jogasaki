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
#include <jogasaki/accessor/const_record_ref.h>

#include <jogasaki/constants.h>
#include <jogasaki/utils/assert.h>

namespace jogasaki::accessor {

bool const_record_ref::is_null(const_record_ref::offset_type nullity_offset) const {
    assert_with_exception(nullity_offset / bits_per_byte < size_, nullity_offset, size_);
    offset_type byte_offset = nullity_offset / bits_per_byte;
    offset_type offset_in_byte = nullity_offset % bits_per_byte;
    unsigned char bitmask = 1U << offset_in_byte;
    auto p = static_cast<unsigned char const*>(data_) + byte_offset; //NOLINT
    return (*p & bitmask) != 0;
}

}  // namespace jogasaki::accessor
