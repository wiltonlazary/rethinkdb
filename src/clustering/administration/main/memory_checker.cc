// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "clustering/administration/main/memory_checker.hpp"

#include <math.h>

#include "clustering/administration/metadata.hpp"
#include "clustering/table_manager/table_meta_client.hpp"
#include "logger.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/pseudo_time.hpp"

static const int64_t delay_time = 5*1000; // TODO 60 sec

static const int64_t reset_time = 30*1000; // TODO 1 hr
memory_checker_t::memory_checker_t(rdb_context_t *_rdb_ctx) :
    rdb_ctx(_rdb_ctx),
    timer(delay_time, this),
    refresh_time(0),
    swap_usage(0)
{
    rassert(rdb_ctx != NULL);
    coro_t::spawn_sometime(std::bind(&memory_checker_t::do_check,
                                     this,
                                     drainer.lock()));
}

void memory_checker_t::do_check(auto_drainer_t::lock_t keepalive) {
    ql::env_t env(rdb_ctx,
                  ql::return_empty_normal_batches_t::NO,
                  keepalive.get_drain_signal(),
                  ql::global_optargs_t(),
                  nullptr);

    uint64_t new_swap_usage = get_used_swap();

#if defined(__MACH__)
    // This is because mach won't give us the swap used by our process.
    if (first_check) {
        swap_usage = new_swap_usage;
        first_check = false;
    }
#endif

#if defined(__MACH__)
    const std::string error_message =
        "Data from a process on this server"
        " has been placed into swap memory in the past hour."
        " If the data is from RethinkDB, this may impact performace.";
#else
    const std::string error_message =
        "Some RethinkDB data on this server"
        " has been placed into swap memory in the past hour."
        " This may impact performance.";
#endif

    if (new_swap_usage > swap_usage) {
        // We've started using more swap
#if defined(__MACH__)
        logWRN("Data from a process on this server"
	       " has been placed into swap memory."
	       " If the data is from RethinkDB, this may impact performace.");
#else
        logWRN("Some RethinkDB data on this server"
        " has been placed into swap memory."
        " This may impact performance.");
#endif
        swap_usage = new_swap_usage;
        refresh_time = 1;
        memory_issue_tracker.report_error(error_message);
    } else if (refresh_time > reset_time) {
        // We've stopped using swap
        logNTC("It has been an hour since data has been placed in swap memory.");
        swap_usage = 0;
#if defined(__MACH__)
        first_check = true;
#endif
        refresh_time = 0;
        memory_issue_tracker.report_success();
    }


    if (refresh_time != 0 && refresh_time <= reset_time) {
        refresh_time += delay_time;
    }
}

