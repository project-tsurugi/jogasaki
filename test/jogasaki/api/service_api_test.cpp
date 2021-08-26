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

#include <sstream>
#include <gtest/gtest.h>

#include <msgpack.hpp>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/utils/binary_printer.h>

#include <tateyama/api/endpoint/mock/endpoint_impls.h>
#include <tateyama/api/endpoint/service.h>
#include "api_test_base.h"

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "schema.pb.h"

namespace jogasaki::api {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace tateyama::api::endpoint;

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;
std::string serialize(::request::Request& r);
void deserialize(std::string_view s, ::response::Response& res);

template <typename T>
static inline bool extract(std::string_view data, T &v, std::size_t &offset) {
    // msgpack::unpack() may throw msgpack::unpack_error with "parse error" or "insufficient bytes" message.
    msgpack::unpacked result = msgpack::unpack(data.data(), data.size(), offset);
    const msgpack::object obj(result.get());
    if (obj.type == msgpack::type::NIL) {
        return false;
    }
    v = obj.as<T>();
    return true;
}

void set_null(accessor::record_ref ref, std::size_t index, meta::record_meta& meta) {
    ref.set_null(meta.nullity_offset(index), true);
}

template <typename T>
static inline void set_value(
    std::string_view data,
    std::size_t &offset,
    accessor::record_ref ref,
    std::size_t index,
    meta::record_meta& meta
) {
    T v;
    if (extract(data, v, offset)) {
        ref.set_value(meta.value_offset(index), v);
    } else {
        set_null(ref, index, meta);
    }
}

jogasaki::meta::record_meta create_record_meta(::schema::RecordMeta const& proto) {
    std::vector<meta::field_type> fields{};
    boost::dynamic_bitset<std::uint64_t> nullities;
    for(std::size_t i=0, n=proto.columns_size(); i<n; ++i) {
        auto& c = proto.columns(i);
        bool nullable = c.nullable();
        meta::field_type field{};
        nullities.push_back(nullable);
        switch(c.type()) {
            using kind = meta::field_type_kind;
            case ::common::DataType::INT4: fields.emplace_back(meta::field_enum_tag<kind::int4>); break;
            case ::common::DataType::INT8: fields.emplace_back(meta::field_enum_tag<kind::int8>); break;
            case ::common::DataType::FLOAT4: fields.emplace_back(meta::field_enum_tag<kind::float4>); break;
            case ::common::DataType::FLOAT8: fields.emplace_back(meta::field_enum_tag<kind::float8>); break;
            case ::common::DataType::CHARACTER: fields.emplace_back(meta::field_enum_tag<kind::character>); break;
        }
    }
    jogasaki::meta::record_meta meta{std::move(fields), std::move(nullities)};
    return meta;
}

std::vector<mock::basic_record> deserialize_msg(std::string_view data, jogasaki::meta::record_meta& meta) {
    std::vector<mock::basic_record> ret{};
    std::size_t offset{};
    while(offset < data.size()) {
        auto& record = ret.emplace_back(maybe_shared_ptr{&meta});
        auto ref = record.ref();
        for (std::size_t index = 0, n = meta.field_count(); index < n ; index++) {
            switch (meta.at(index).kind()) {
                case jogasaki::meta::field_type_kind::int4: set_value<std::int32_t>(data, offset, ref, index, meta); break;
                case jogasaki::meta::field_type_kind::int8: set_value<std::int64_t>(data, offset, ref, index, meta); break;
                case jogasaki::meta::field_type_kind::float4: set_value<float>(data, offset, ref, index, meta); break;
                case jogasaki::meta::field_type_kind::float8: set_value<double>(data, offset, ref, index, meta); break;
                case jogasaki::meta::field_type_kind::character: {
                    std::string v;
                    if (extract(data, v, offset)) {
                        record.ref().set_value(meta.value_offset(index), accessor::text{v});
                    } else {
                        set_null(record.ref(), index, meta);
                    }
                    break;
                }
                default:
                    std::abort();
            }
        }
    }
    return ret;
}

class service_api_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->single_thread(true);
        db_setup(cfg);
        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
        service_ = tateyama::api::endpoint::create_service(*db_);
    }

    void TearDown() override {
        db_teardown();
    }
    void test_begin(std::uint64_t& handle);
    void test_commit(std::uint64_t& handle);

    template <class ...Args>
    void test_prepare(std::uint64_t& handle, std::string sql, Args...args) {
        std::vector<std::pair<std::string, common::DataType>> place_holders{args...};
        ::request::Request r{};
        auto* p = r.mutable_prepare();
        p->mutable_sql()->assign(sql);
        if (! place_holders.empty()) {
            auto vars = p->mutable_host_variables();
            for(auto&& [n, t] : place_holders) {
                auto* v = vars->add_variables();
                v->set_name(n);
                v->set_type(t);
            }
        }
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_prepare());
        auto& prep = resp.prepare();
        ASSERT_TRUE(prep.has_prepared_statement_handle());
        auto& stmt = prep.prepared_statement_handle();
        handle = stmt.handle();
    }
    void test_dispose_prepare(std::uint64_t& handle);

    std::unique_ptr<tateyama::api::endpoint::service> service_{};  //NOLINT
};

using namespace std::string_view_literals;

std::string serialize(::request::Request& r) {
    std::string s{};
    if (!r.SerializeToString(&s)) {
        std::abort();
    }
//    std::cout << " DebugString : " << r.DebugString() << std::endl;
//    std::cout << " Binary data : " << utils::binary_printer{s.data(), s.size()} << std::endl;
    return s;
}

void deserialize(std::string_view s, ::response::Response& res) {
    if (!res.ParseFromString(std::string(s))) {
        std::abort();
    }
//    std::cout << " Binary data : " << utils::binary_printer{s.data(), s.size()} << std::endl;
//    std::cout << " DebugString : " << res.DebugString() << std::endl;
}

void service_api_test::test_begin(std::uint64_t& handle) {
    ::request::Request r{};
    r.mutable_begin()->set_read_only(false);
    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);

    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

    auto st = (*service_)(req, res);
    // TODO the operation can be asynchronous. Wait until response becomes ready.
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);

    ::response::Response resp{};
    deserialize(res->body_, resp);
    ASSERT_TRUE(resp.has_begin());
    auto& begin = resp.begin();
    ASSERT_TRUE(begin.has_transaction_handle());
    auto& tx = begin.transaction_handle();
    handle = tx.handle();
}

void service_api_test::test_commit(std::uint64_t& handle) {
    ::request::Request r{};
    r.mutable_commit()->mutable_transaction_handle()->set_handle(handle);
    auto s = serialize(r);

    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

    auto st = (*service_)(req, res);
    // TODO the operation can be asynchronous. Wait until response becomes ready.
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);
}

TEST_F(service_api_test, begin_and_commit) {
    std::uint64_t handle{};
    test_begin(handle);
    test_commit(handle);
}

TEST_F(service_api_test, rollback) {
    std::uint64_t handle{};
    test_begin(handle);
    {
        ::request::Request r{};
        r.mutable_rollback()->mutable_transaction_handle()->set_handle(handle);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
    }
}

void service_api_test::test_dispose_prepare(std::uint64_t& handle) {
    ::request::Request r{};
    r.mutable_dispose_prepared_statement()->mutable_prepared_statement_handle()->set_handle(handle);
    auto s = serialize(r);

    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

    auto st = (*service_)(req, res);
    // TODO the operation can be asynchronous. Wait until response becomes ready.
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);
}

TEST_F(service_api_test, prepare_and_dispose) {
    std::uint64_t handle{};
    test_prepare(handle, "select * from T1");
    test_dispose_prepare(handle);
}

TEST_F(service_api_test, disconnect) {
    std::uint64_t handle{};
    {
        ::request::Request r{};
        r.mutable_disconnect();
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_result_only());
        auto& ro = resp.result_only();
        ASSERT_TRUE(ro.has_success());
    }
}

TEST_F(service_api_test, execute_statement_and_query) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_statement();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_sql()->assign("insert into T0(C0, C1) values (1, 10.0)");
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_result_only());
        auto& ro = resp.result_only();
        ASSERT_TRUE(ro.has_success());
    }
    test_commit(tx_handle);
    test_begin(tx_handle);
    {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_query();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_sql()->assign("select * from T0");
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_execute_query());
        auto& eq = resp.execute_query();

        ASSERT_FALSE(eq.has_error());
        ASSERT_TRUE(eq.has_result_set_info());
        auto& rsinfo = eq.result_set_info();
        std::cout << "name : " << rsinfo.name() << std::endl;
        ASSERT_TRUE(rsinfo.has_record_meta());
        auto meta = rsinfo.record_meta();
        ASSERT_EQ(2, meta.columns_size());
        meta.columns(0).type();

        EXPECT_EQ(::common::DataType::INT8, meta.columns(0).type());
        EXPECT_TRUE(meta.columns(0).nullable());
        EXPECT_EQ(::common::DataType::FLOAT8, meta.columns(1).type());
        EXPECT_TRUE(meta.columns(1).nullable());
        {
            ASSERT_TRUE(res->channel_);
            auto& ch = *res->channel_;
            ASSERT_EQ(1, ch.buffers_.size());
            ASSERT_TRUE(ch.buffers_[0]);
            auto& buf = *ch.buffers_[0];
            auto m = create_record_meta(meta);
            auto v = deserialize_msg(std::string_view{buf.data_, buf.size_}, m);
            ASSERT_EQ(1, v.size());
            auto exp = mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0);
            EXPECT_EQ(exp, v[0]);
        }
    }
    test_commit(tx_handle);
}

TEST_F(service_api_test, execute_prepared_statement_and_query) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0(C0, C1) values (:c0, :c1)",
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_prepared_statement();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_prepared_statement_handle()->set_handle(stmt_handle);
        auto* params = stmt->mutable_parameters();
        auto* c0 = params->add_parameters();
        c0->set_name("c0");
        c0->set_int8_value(1);
        auto* c1 = params->add_parameters();
        c1->set_name("c1");
        c1->set_float8_value(10.0);
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_result_only());
        auto& ro = resp.result_only();
        ASSERT_TRUE(ro.has_success());
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    test_begin(tx_handle);
    {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_prepared_query();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_prepared_statement_handle()->set_handle(query_handle);
        auto* params = stmt->mutable_parameters();
        auto* c0 = params->add_parameters();
        c0->set_name("c0");
        c0->set_int8_value(1);
        auto* c1 = params->add_parameters();
        c1->set_name("c1");
        c1->set_float8_value(10.0);
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_execute_query());
        auto& eq = resp.execute_query();

        ASSERT_FALSE(eq.has_error());
        ASSERT_TRUE(eq.has_result_set_info());
        auto& rsinfo = eq.result_set_info();
        std::cout << "name : " << rsinfo.name() << std::endl;
        ASSERT_TRUE(rsinfo.has_record_meta());
        auto meta = rsinfo.record_meta();
        ASSERT_EQ(2, meta.columns_size());

        EXPECT_EQ(::common::DataType::INT8, meta.columns(0).type());
        EXPECT_TRUE(meta.columns(0).nullable());
        EXPECT_EQ(::common::DataType::FLOAT8, meta.columns(1).type());
        EXPECT_TRUE(meta.columns(1).nullable());
        {
            ASSERT_TRUE(res->channel_);
            auto& ch = *res->channel_;
            ASSERT_EQ(1, ch.buffers_.size());
            ASSERT_TRUE(ch.buffers_[0]);
            auto& buf = *ch.buffers_[0];
            ASSERT_LT(0, buf.size_);
            auto m = create_record_meta(meta);
            auto v = deserialize_msg(std::string_view{buf.data_, buf.size_}, m);
            ASSERT_EQ(1, v.size());
            auto exp = mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0);
            EXPECT_EQ(exp, v[0]);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, msgpack1) {
    // verify msgpack behavior
    using namespace std::string_view_literals;
    std::stringstream ss;
    {
        msgpack::pack(ss, msgpack::type::nil_t()); // nil can be put without specifying the type
        std::int32_t i32{1};
        msgpack::pack(ss, i32);
        i32 = 100000;
        msgpack::pack(ss, i32);
        std::int64_t i64{2};
        msgpack::pack(ss, i64);
        float f4{10.0};
        msgpack::pack(ss, f4);
        float f8{11.0};
        msgpack::pack(ss, f8);
        msgpack::pack(ss, "ABC"sv);
    }

    std::string str{ss.str()};
    std::size_t offset{};
    std::int32_t i32{};
    std::int64_t i64{};
    EXPECT_FALSE(extract(str, i32, offset));  // nil can be read as any type
    ASSERT_EQ(1, offset);
    EXPECT_TRUE(extract(str, i32, offset));
    EXPECT_EQ(1, i32);
    ASSERT_EQ(2, offset);
    EXPECT_TRUE(extract(str, i32, offset));
    EXPECT_EQ(100000, i32);
    ASSERT_EQ(7, offset);
    EXPECT_TRUE(extract(str, i64, offset));
    EXPECT_EQ(2, i64);
    ASSERT_EQ(8, offset);
}

TEST_F(service_api_test, data_types) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T1(C0, C1, C2, C3, C4) values (:c0, :c1, :c2, :c3, c4)",
        std::pair{"c0"s, ::common::DataType::INT4},
        std::pair{"c1"s, ::common::DataType::INT8},
        std::pair{"c2"s, ::common::DataType::FLOAT8},
        std::pair{"c3"s, ::common::DataType::FLOAT4},
        std::pair{"c4"s, ::common::DataType::CHARACTER}
    );
    for(std::size_t i=0; i < 3; ++i) {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_prepared_statement();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_prepared_statement_handle()->set_handle(stmt_handle);
        r.mutable_session_handle()->set_handle(1);

        auto* params = stmt->mutable_parameters();
        auto* c0 = params->add_parameters();
        c0->set_name("c0");
        c0->set_int4_value(i);
        auto* c1 = params->add_parameters();
        c1->set_name("c1");
        c1->set_int8_value(i);
        auto* c2 = params->add_parameters();
        c2->set_name("c2");
        c2->set_float8_value(i);
        auto* c3 = params->add_parameters();
        c3->set_name("c3");
        c3->set_float4_value(i);
        auto* c4 = params->add_parameters();
        c4->set_name("c4");
        c4->set_character_value(std::to_string(i));

        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_result_only());
        auto& ro = resp.result_only();
        ASSERT_TRUE(ro.has_success());
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, C1, C2, C3, C4 from T1 where C1 > :c1 and C2 > :c2 and C4 > :c4 order by C0",
        std::pair{"c1"s, ::common::DataType::INT8},
        std::pair{"c2"s, ::common::DataType::FLOAT8},
        std::pair{"c4"s, ::common::DataType::CHARACTER}
    );
    test_begin(tx_handle);
    {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_prepared_query();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_prepared_statement_handle()->set_handle(query_handle);
        auto* params = stmt->mutable_parameters();
        auto* c1 = params->add_parameters();
        c1->set_name("c1");
        c1->set_int8_value(0);
        auto* c2 = params->add_parameters();
        c2->set_name("c2");
        c2->set_float8_value(0.0);
        auto* c4 = params->add_parameters();
        c4->set_name("c4");
        c4->set_character_value("0");
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_execute_query());
        auto& eq = resp.execute_query();

        ASSERT_FALSE(eq.has_error());
        ASSERT_TRUE(eq.has_result_set_info());
        auto& rsinfo = eq.result_set_info();
        std::cout << "name : " << rsinfo.name() << std::endl;
        ASSERT_TRUE(rsinfo.has_record_meta());
        auto meta = rsinfo.record_meta();
        ASSERT_EQ(5, meta.columns_size());

        EXPECT_EQ(::common::DataType::INT4, meta.columns(0).type());
        EXPECT_TRUE(meta.columns(0).nullable()); //TODO
        EXPECT_EQ(::common::DataType::INT8, meta.columns(1).type());
        EXPECT_TRUE(meta.columns(1).nullable());
        EXPECT_EQ(::common::DataType::FLOAT8, meta.columns(2).type());
        EXPECT_TRUE(meta.columns(2).nullable());
        EXPECT_EQ(::common::DataType::FLOAT4, meta.columns(3).type());
        EXPECT_TRUE(meta.columns(3).nullable());
        EXPECT_EQ(::common::DataType::CHARACTER, meta.columns(4).type());
        EXPECT_TRUE(meta.columns(4).nullable());
        {
            ASSERT_TRUE(res->channel_);
            auto& ch = *res->channel_;
            ASSERT_EQ(1, ch.buffers_.size());
            ASSERT_TRUE(ch.buffers_[0]);
            auto& buf = *ch.buffers_[0];
            ASSERT_LT(0, buf.size_);
            std::cout << "buf size : " << buf.size_ << std::endl;
            auto m = create_record_meta(meta);
            auto v = deserialize_msg(std::string_view{buf.data_, buf.size_}, m);
            ASSERT_EQ(2, v.size());
            auto exp1 = mock::create_nullable_record<meta::field_type_kind::int4, meta::field_type_kind::int8, meta::field_type_kind::float8, meta::field_type_kind::float4, meta::field_type_kind::character>(1, 1, 1.0, 1.0, accessor::text{"1"sv});
            auto exp2 = mock::create_nullable_record<meta::field_type_kind::int4, meta::field_type_kind::int8, meta::field_type_kind::float8, meta::field_type_kind::float4, meta::field_type_kind::character>(2, 2, 2.0, 2.0, accessor::text{"2"sv});
            EXPECT_EQ(exp1, v[0]);
            EXPECT_EQ(exp2, v[1]);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, protobuf1) {
    // verify protobuf behavior
    using namespace std::string_view_literals;
    std::stringstream ss;
    ::request::Request req{};
    EXPECT_FALSE(req.has_begin());
    EXPECT_FALSE(req.has_session_handle());
    auto& h = req.session_handle();
    EXPECT_EQ(0, h.handle());  // default object has zero handle, that means empty
    auto* session = req.mutable_session_handle();
    EXPECT_TRUE(req.has_session_handle());
    req.clear_session_handle();
    EXPECT_FALSE(req.has_session_handle());

    ::common::Session s;
    req.set_allocated_session_handle(&s);
    EXPECT_TRUE(req.has_session_handle());
    req.release_session_handle();
    EXPECT_FALSE(req.has_session_handle());
}

TEST_F(service_api_test, invalid_request) {
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>("ABC");
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    EXPECT_EQ(tateyama::status::ok, st);
    EXPECT_NE(response_code::success, res->code_);
}

}
