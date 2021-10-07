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
#include <jogasaki/executor/process/result_store_writer.h>

#include <string>

#include <gtest/gtest.h>

#include <jogasaki/test_root.h>
#include <jogasaki/mock_memory_resource.h>
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor::process {

using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace jogasaki::mock;
using namespace boost::container::pmr;

class result_store_writer_test : public test_root {};

TEST_F(result_store_writer_test, basic) {

    mock_memory_resource record_resource{};
    mock_memory_resource varlen_resource{};

    using kind = meta::field_type_kind;
    using test_record = jogasaki::mock::basic_record;
    auto meta = create_meta<kind::int4, kind::float8, kind::int8>();
    data::iterable_record_store store{
        &record_resource,
        &varlen_resource,
        meta
    };
    result_store_writer writer{store, meta};

    test_record rec1{create_record<kind::int4, kind::float8, kind::int8>(1, 10.0, 100)};
    test_record rec2{create_record<kind::int4, kind::float8, kind::int8>(2, 20.0, 200)};
    auto record_size = meta->record_size();
    writer.write(rec1.ref());
    writer.write(rec2.ref());

    compare_info cm{*meta};
    comparator comp{cm};
    auto b = store.begin();
    EXPECT_EQ(0, comp(rec1.ref(), b.ref()));
    ++b;
    EXPECT_EQ(0, comp(rec2.ref(), b.ref()));
    ++b;
    EXPECT_EQ(store.end(), b);
}

}

