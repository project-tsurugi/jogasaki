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

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <boost/dynamic_bitset.hpp>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/statement/write.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/plan/process.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/fail.h>

#include <yugawara/compiler_result.h>

namespace jogasaki::testing {

template<class T, class Port>
inline T& next(Port& port) {
    if (!port.opposite()) {
        throw std::domain_error("not connected");
    }
    auto&& r = port.opposite()->owner();
    if (r.kind() != T::tag) {
        throw std::domain_error(takatori::util::string_builder {}
                << r.kind()
                << " <=> "
                << T::tag
                << takatori::util::string_builder::to_string);
    }
    return takatori::util::downcast<T>(r);
}

template<class T>
inline T& last(::takatori::relation::graph_type& graph) {
    for (auto&& e : graph) {
        if (e.output_ports().empty()) {
            return takatori::util::downcast<T>(e);
        }
    }
    takatori::util::fail();
}

template<class T>
inline T& head(::takatori::relation::graph_type& graph) {
    T* result = nullptr;
    ::takatori::relation::enumerate_top(graph, [&](::takatori::relation::expression& v) {
        result = takatori::util::downcast<T>(&v);
    });
    if (result != nullptr) {
        return *result;
    }
    throw std::domain_error(takatori::util::string_builder {}
            << "missing head: "
            << T::tag
            << takatori::util::string_builder::to_string);
}

inline takatori::plan::process&
find(takatori::plan::graph_type& g, takatori::relation::expression const& e) {
    for (auto&& s : g) {
        if (s.kind() == takatori::plan::step_kind::process) {
            auto&& p = takatori::util::downcast<takatori::plan::process>(s);
            if (p.operators().contains(e)) {
                return p;
            }
        }
    }
    throw std::domain_error(takatori::util::string_builder {}
            << "missing process that contain: "
            << e
            << takatori::util::string_builder::to_string);
}

static void dump(yugawara::compiler_result const& r) {
    ::takatori::serializer::json_printer printer { std::cout };
    r.object_scanner()(
            r.statement(),
            ::takatori::serializer::json_printer { std::cout });
}

}
