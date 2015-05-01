// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/query_server.hpp"

#include "concurrency/cross_thread_watchable.hpp"
#include "concurrency/watchable.hpp"
#include "perfmon/perfmon.hpp"
#include "rdb_protocol/backtrace.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/profile.hpp"
#include "rdb_protocol/query.hpp"
#include "rdb_protocol/query_cache.hpp"
#include "rdb_protocol/response.hpp"
#include "rpc/semilattice/view/field.hpp"

rdb_query_server_t::rdb_query_server_t(const std::set<ip_address_t> &local_addresses,
                                       int port,
                                       rdb_context_t *_rdb_ctx) :
    server(_rdb_ctx, local_addresses, port, this, default_http_timeout_sec),
    rdb_ctx(_rdb_ctx),
    thread_counters(0) { }

http_app_t *rdb_query_server_t::get_http_app() {
    return &server;
}

int rdb_query_server_t::get_port() const {
    return server.get_port();
}

// Predeclaration for run, only used here
namespace ql {
    void run(const query_params_t &query_params,
             response_t *response_out,
             signal_t *interruptor);
}

void rdb_query_server_t::run_query(const ql::query_params_t &query_params,
                                   ql::response_t *response_out,
                                   signal_t *interruptor) {
    guarantee(interruptor != nullptr);
    guarantee(rdb_ctx->cluster_interface != nullptr);
    try {
        scoped_perfmon_counter_t client_active(&rdb_ctx->stats.clients_active); // TODO: make this correct for parallelized queries
        // `ql::run` will set the status code
        ql::run(query_params, response_out, interruptor);
    } catch (const interrupted_exc_t &ex) {
        throw; // Interruptions should be handled by our caller, who can provide context
#ifdef NDEBUG // In debug mode we crash, in release we send an error.
    } catch (const std::exception &e) {
        response_out->fill_error(Response::RUNTIME_ERROR,
                                 strprintf("Unexpected exception: %s\n", e.what()),
                                 ql::backtrace_registry_t::EMPTY_BACKTRACE);
#endif // NDEBUG
    }

    rdb_ctx->stats.queries_per_sec.record();
    ++rdb_ctx->stats.queries_total;
}
