// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "arch/runtime/coroutines.hpp"

#include <stdexcept>

#include "arch/runtime/runtime.hpp"
#include "arch/timing.hpp"
#include "concurrency/cond_var.hpp"
#include "unittest/gtest.hpp"
#include "unittest/unittest_utils.hpp"

namespace unittest {

void run_in_coro(const std::function<void()> &fun) {
    run_in_thread_pool([&]() {
        // `run_in_thread_pool` already spawns a coroutine for us.
        ASSERT_NE(coro_t::self(), nullptr);
        fun();
    });
}

TEST(CoroutineUtilsTest, WithEnoughStackNoSpawn) {
    int res = 0;
    run_in_coro([&]() {
        // This should execute directly
        ASSERT_NO_CORO_WAITING;
        res = call_with_enough_stack<int>([]() {
            return 5;
        }, 1);
    });
    EXPECT_EQ(res, 5);
}

TEST(CoroutineUtilsTest, WithEnoughStackNonBlocking) {
    int res = 0;
    run_in_coro([&]() {
        ASSERT_FINITE_CORO_WAITING;
        // `COROUTINE_STACK_SIZE` forces a coroutine to be spawned
        res = call_with_enough_stack<int>([]() {
            return 5;
        }, COROUTINE_STACK_SIZE);
    });
    EXPECT_EQ(res, 5);
}

TEST(CoroutineUtilsTest, WithEnoughStackBlocking) {
    int res = 0;
    run_in_coro([&]() {
        // `COROUTINE_STACK_SIZE` forces a coroutine to be spawned
        res = call_with_enough_stack<int>([]() {
            nap(5);
            return 5;
        }, COROUTINE_STACK_SIZE);
    });
    EXPECT_EQ(res, 5);
}

TEST(CoroutineUtilsTest, WithEnoughStackNoCoro) {
    // call_with_enough_stack should still be usable if we are not in a coroutine
    // (though it doesn't do much in that case).
    int res = 0;
    run_in_thread_pool([&]() {
        struct test_message_t : public linux_thread_message_t {
            void on_thread_switch() {
                ASSERT_EQ(coro_t::self(), nullptr);
                *out = call_with_enough_stack<int>([]() {
                    return 5;
                }, 1);
                done_cond.pulse();
            }
            int *out;
            cond_t done_cond;
        };
        test_message_t thread_msg;
        thread_msg.out = &res;
        call_later_on_this_thread(&thread_msg);
        thread_msg.done_cond.wait();
    });
    EXPECT_EQ(res, 5);
}

TEST(CoroutineUtilsTest, WithEnoughStackException) {
    bool got_exception = false;
    run_in_coro([&]() {
        try {
            // `COROUTINE_STACK_SIZE` forces a coroutine to be spawned
            call_with_enough_stack([]() {
                throw std::runtime_error("This is a test exception");
            }, COROUTINE_STACK_SIZE);
        } catch (const std::exception &) {
            got_exception = true;
        }
    });
    EXPECT_TRUE(got_exception);
}

}   /* namespace unittest */
