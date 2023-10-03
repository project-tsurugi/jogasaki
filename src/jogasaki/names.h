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

#include <unordered_map>

#include <takatori/descriptor/variable.h>

namespace jogasaki {

///@brief short name for vector of variables
using variable_vector = std::vector<takatori::descriptor::variable>;

/**
 * @brief hash table whose elements may or may not be relocated
 * @details The user should not keep the references or iterators to elements for later use because relocation can happen
 * and they might be invalidated. As there is no restriction on the relocation, implementation can be replaced
 * in the future with other hash map such as hopscotch.
 */
template<class Key, class T,
    class Hash = std::hash<Key>,
    class Pred = std::equal_to<Key>,
    class Alloc = std::allocator<std::pair<const Key, T>>>
using hash_table = std::unordered_map<Key, T, Hash, Pred, Alloc>;

/**
 * @brief hash table whose elements may not be relocated
 * @details The user can keep the references or iterators to elements for later use because relocation never happens
 * and they are not invalidated. The implementation hash map should be chosen so that the relocation doesn't happen.
 */
template<class Key, class T,
    class Hash = std::hash<Key>,
    class Pred = std::equal_to<Key>,
    class Alloc = std::allocator<std::pair<const Key, T>>>
using stable_hash_table = std::unordered_map<Key, T, Hash, Pred, Alloc>;

}

