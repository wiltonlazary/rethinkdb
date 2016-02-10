// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_MAIN_MEMORY_CHECKER_HPP_
#define CLUSTERING_ADMINISTRATION_MAIN_MEMORY_CHECKER_HPP_

#include <functional>

#include "errors.hpp"
#include <boost/shared_ptr.hpp>

#include "arch/runtime/coroutines.hpp"
#include "arch/timing.hpp"
#include "clustering/administration/issues/memory.hpp"
#include "clustering/administration/main/cache_size.hpp"
#include "concurrency/auto_drainer.hpp"
#include "rdb_protocol/context.hpp"

class server_config_client_t; //TODO, need it?
class table_meta_client_t;

enum class memory_check_t {
    do_not_perform,
    perform
};

class memory_checker_t : private repeating_timer_callback_t {
public:
    memory_checker_t(rdb_context_t *_rdb_ctx);

    memory_issue_tracker_t *get_memory_issue_tracker() {
        return &memory_issue_tracker;
    }
private:
    void do_check(auto_drainer_t::lock_t keepalive);
    void on_ring() final {
        coro_t::spawn_sometime(std::bind(&memory_checker_t::do_check,
                                         this,
                                         drainer.lock()));
    }
    rdb_context_t *const rdb_ctx;

    auto_drainer_t drainer;
    repeating_timer_t timer;

    memory_issue_tracker_t memory_issue_tracker;

    uint64_t refresh_time;
    uint64_t swap_usage;

    bool print_log_message;

#if defined(__MACH__)
    bool first_check;
#endif
};

#endif // CLUSTERING_ADMINISTRATION_MAIN_MEMORY_CHECKER_HPP_
