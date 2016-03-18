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

}  // namespace unittest

