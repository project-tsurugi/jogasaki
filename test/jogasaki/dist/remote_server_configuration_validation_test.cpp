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
#include <sstream>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/dist/remote_server_channel_manager.h>
#include <jogasaki/dist/remote_server_configuration_validation.h>
#include <jogasaki/dist/test_root_certificate.h>
#include <jogasaki/configuration.h>
#include <tateyama/api/configuration.h>

namespace jogasaki::testing {
TEST(remote_server_configuration_validation_test, accepts_single_dns_and_absolute_plain_unix_targets) {
    for (auto uri : {"dns:///worker:1", "dns:///worker.example:65535", "dns:///127.0.0.1:0080", "dns:///[::1]:12345"}) {
        EXPECT_TRUE(dist::valid_remote_server_uri(uri, false)) << uri;
        EXPECT_TRUE(dist::valid_remote_server_uri(uri, true)) << uri;
    }
    EXPECT_TRUE(dist::valid_remote_server_uri("unix:///tmp/dq.sock", false));
}
TEST(remote_server_configuration_validation_test, rejects_malformed_or_multiple_dns_targets) {
    for (auto uri : {"dns:///", "dns:///host", "dns:///:12345", "dns:///host:", "dns:///host:0", "dns:///host:65536",
        "dns:///host:9999999999999999", "dns:///host:+1", "dns:///host:-1", "dns:///host:1x", "dns:///host:12:13",
        "dns:///a:1,b:2", "dns://authority/host:1", "dns:///user@host:1", "dns:///host:1/path", "dns:///host:1?x",
        "dns:///host:1#x", "dns:///ho st:1", "dns:///host%20:1", "dns:///::1:1", "dns:///[broken]:1",
        "dns:///[127.0.0.1]:1", "dns:///[::1]", "dns:///[::1]:0", "DNS:///host:1", "host:1"}) {
        EXPECT_FALSE(dist::valid_remote_server_uri(uri, false)) << uri;
    }
    EXPECT_FALSE(dist::valid_remote_server_uri(std::string{"dns:///h:1\0x", 12}, false));
}
TEST(remote_server_configuration_validation_test, rejects_relative_unix_and_unix_tls) {
    EXPECT_FALSE(dist::valid_remote_server_uri("unix://relative", false));
    EXPECT_FALSE(dist::valid_remote_server_uri("unix:path", false));
    EXPECT_FALSE(dist::valid_remote_server_uri("unix:///tmp/dq.sock", true));
}
TEST(remote_server_configuration_validation_test, parses_every_certificate_in_a_bundle) {
    auto certificate = std::string{remote_test_root_certificate};
    EXPECT_TRUE(dist::valid_remote_root_certificates(certificate));
    EXPECT_TRUE(dist::valid_remote_root_certificates(certificate + certificate));
    std::string crlf{};
    for (char c : certificate) { if (c == '\n') crlf += '\r'; crlf += c; }
    EXPECT_TRUE(dist::valid_remote_root_certificates(crlf));
    EXPECT_FALSE(dist::valid_remote_root_certificates(certificate + "x" + certificate));
    EXPECT_FALSE(dist::valid_remote_root_certificates(certificate + " " + certificate));
    auto garbage = certificate;
    garbage.insert(garbage.find("-----END CERTIFICATE-----") + std::string{"-----END CERTIFICATE-----"}.size(), "garbage");
    EXPECT_FALSE(dist::valid_remote_root_certificates(certificate + garbage + certificate));
    std::string broken{"-----BEGIN CERTIFICATE-----\nnot-a-certificate\n-----END CERTIFICATE-----\n"};
    EXPECT_FALSE(dist::valid_remote_root_certificates(broken));
    EXPECT_FALSE(dist::valid_remote_root_certificates(certificate + broken));
    EXPECT_FALSE(dist::valid_remote_root_certificates(broken + certificate));
    EXPECT_FALSE(dist::valid_remote_root_certificates(certificate + "-----BEGIN CERTIFICATE-----\ntruncated"));
    EXPECT_FALSE(dist::valid_remote_root_certificates("-----BEGIN PRIVATE KEY-----\nunused\n-----END PRIVATE KEY-----\n" + certificate));
    EXPECT_FALSE(dist::valid_remote_root_certificates(""));
}
TEST(remote_server_configuration_validation_test, rejects_inconsistent_direct_credentials_without_publication) {
    using source = dist::root_certificate_source;
    for (auto const& config : std::vector<dist::remote_server_configuration>{
        {"worker", "localhost:12345", false, source::pem, std::string{remote_test_root_certificate}},
        {"worker", "localhost:12345", false, source::grpc_default_roots, {}},
        {"worker", "localhost:12345", true, source::none, {}},
        {"worker", "unix:/tmp/dq.sock", true, source::grpc_default_roots, {}},
        {"worker", "localhost:12345", true, source::grpc_default_roots, "unexpected"},
        {"worker", "localhost:12345", true, source::pem, "not-a-certificate"}}) {
        dist::remote_server_channel_manager channels{};
        EXPECT_EQ(status::err_invalid_argument, channels.start({{"first", "localhost:12345", false, source::none, {}}, config}));
        EXPECT_FALSE(channels.find_channel("first"));
        EXPECT_FALSE(channels.find_channel("worker"));
        EXPECT_EQ(status::ok, channels.start({{"worker", "localhost:12345", false, source::none, {}}}));
        channels.stop();
    }
}
TEST(remote_server_configuration_validation_test, accepts_consistent_credentials) {
    using source = dist::root_certificate_source;
    EXPECT_TRUE(dist::valid_remote_server_credentials({"worker", "localhost:1", false, source::none, {}}));
    EXPECT_TRUE(dist::valid_remote_server_credentials({"worker", "localhost:1", true, source::grpc_default_roots, {}}));
    EXPECT_TRUE(dist::valid_remote_server_credentials({"worker", "localhost:1", true, source::pem, std::string{remote_test_root_certificate}}));
}
TEST(remote_server_configuration_validation_test, ini_rejects_reserved_names_invalid_uri_and_unix_tls) {
    for (auto const& options : std::vector<std::string>{
        "name=__worker\nuri=dns:///host:12345\n", "name=worker\nuri=dns:///host\n",
        "name=worker\nuri=dns:///host:0\n", "name=worker\nuri=unix:///tmp/dq.sock\nsecure=true\n"}) {
        std::stringstream input{"[distributed_query_coordinator]\nenabled=true\n[distributed_query_client]\n" + options};
        tateyama::api::configuration::whole config{input, "[distributed_query_coordinator]\nenabled=false\n[distributed_query_client]\nname=\nuri=\n"};
        EXPECT_FALSE(api::resource::convert_config(config)) << options;
    }
}
TEST(remote_server_configuration_validation_test, plain_ini_ignores_unused_certificate_file) {
    std::stringstream input{"[distributed_query_coordinator]\nenabled=true\n[distributed_query_client]\nname=worker\nuri=unix:///tmp/dq.sock\nsecure=false\nroot_cert=does-not-exist.pem\n"};
    tateyama::api::configuration::whole config{input, "[distributed_query_coordinator]\nenabled=false\n[distributed_query_client]\nname=\nuri=\n"};
    auto converted = api::resource::convert_config(config);
    ASSERT_TRUE(converted);
    EXPECT_EQ(dist::root_certificate_source::none, converted->distributed_query_remote_servers().front().root_certificate);
}
TEST(remote_server_configuration_validation_test, counts_channel_lookup_failures_and_releases_channel_gauge) {
    dist::remote_server_channel_manager manager{};
    auto stats = manager.statistics();
    EXPECT_FALSE(manager.find_channel("worker"));
    EXPECT_EQ(1, stats->acquisitions.load());
    EXPECT_EQ(1, stats->failures.load());
    dist::remote_server_configuration cfg{};
    cfg.name = "worker";
    cfg.uri = "dns:///127.0.0.1:49388";
    ASSERT_EQ(status::ok, manager.start({cfg}));
    EXPECT_EQ(1, stats->channels.load());
    EXPECT_TRUE(manager.find_channel("worker"));
    EXPECT_FALSE(manager.find_channel("missing"));
    EXPECT_EQ(3, stats->acquisitions.load());
    EXPECT_EQ(2, stats->failures.load());
    manager.stop();
    EXPECT_EQ(0, stats->channels.load());
    EXPECT_FALSE(manager.find_channel("worker"));
    EXPECT_EQ(4, stats->acquisitions.load());
    EXPECT_EQ(3, stats->failures.load());
}

} // namespace jogasaki::testing
