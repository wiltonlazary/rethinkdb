// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "clustering/administration/main/memory_checker.hpp"

#include <math.h>

#include "clustering/administration/metadata.hpp"
#include "clustering/table_manager/table_meta_client.hpp"
#include "logger.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/pseudo_time.hpp"

static const int64_t delay_time = 5*1000; //TODO 60 sec

memory_checker_t::memory_checker_t(rdb_context_t *_rdb_ctx) :
    rdb_ctx(_rdb_ctx),
    timer(delay_time, this),
    no_swap_usage(true)
#if defined(__MACH__)
    ,pageouts(0),
    first_check(true)
#endif
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
    uint64_t used_swap = get_used_swap();
#if defined(__MACH__)
    // This is because mach won't give us the swap used by our process.
    if (first_check) {
        pageouts = used_swap;
        used_swap = 0;
        first_check = false;
    } else {
        if (used_swap == pageouts) {
            used_swap = 0;
        }
    }
#endif

#if defined(__MACH__)
    const std::string error_message =
        "Data from a process on this server"
        " has been placed into swap memory."
        " If the data is from RethinkDB, this may impact performace.";
#else
    const std::string error_message =
        "Some RethinkDB data on this server"
        " has been placed into swap memory."
        " This may impact performance.";
#endif
    if (used_swap > 0 && no_swap_usage) {
        // We've started using swap
#if defined(__MACH__)
        logWRN("Data from a process on this server"
	       " has been placed into swap memory."
	       " If the data is from RethinkDB, this may impact performace.");
#else
        logWRN("Some RethinkDB data on this server"
        " has been placed into swap memory."
        " This may impact performance.");
#endif
        no_swap_usage = false;
        memory_issue_tracker.report_error(error_message);
    } else if (used_swap == 0 && !no_swap_usage) {
        // We've stopped using swap
        logNTC("This server hs stopped using swap memory.");
        no_swap_usage = true;
        memory_issue_tracker.report_success();
    }
}

