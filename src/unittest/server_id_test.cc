// Copyright 2010-2016 RethinkDB, all rights reserved.

#include <set>

#include "rpc/connectivity/server_id.hpp"
#include "unittest/gtest.hpp"

namespace unittest {

TEST(ServerIdTest, Generate) {
    std::set<server_id_t> ids;
    for (int i = 0; i < 1000; ++i) {
        server_id_t sid = server_id_t::generate_server_id();
        ASSERT_FALSE(sid.is_proxy());
        ids.insert(sid);
        server_id_t pid = server_id_t::generate_proxy_id();
        ASSERT_TRUE(pid.is_proxy());
        ids.insert(pid);
    }
    ASSERT_EQ(ids.size(), 2000);
}

TEST(ServerIdTest, PrintAndParse) {
    for (int i = 0; i < 1000; ++i) {
        server_id_t sid = server_id_t::generate_server_id();
        server_id_t parsed_sid;
        ASSERT_TRUE(str_to_serverid(sid.print(), &parsed_sid));
        ASSERT_EQ(sid, parsed_sid);

        server_id_t pid = server_id_t::generate_proxy_id();;
        server_id_t parsed_pid;
        ASSERT_TRUE(str_to_serverid(pid.print(), &parsed_pid));
        ASSERT_EQ(pid, parsed_pid);
    }
}

}  // namespace unittest

