// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/optargs.hpp"

#include <set>

#include "rdb_protocol/env.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/val.hpp"

namespace ql {

global_optargs_t::global_optargs_t() { }

global_optargs_t::global_optargs_t(counted_t<term_storage_t> term_storage) {
    auto optarg_it = term_storage->global_optargs();
    while (const raw_term_t *optarg = optarg_it.next()) {
        compile_env_t env((var_visibility_t()), term_storage.get());
        counted_t<func_term_t> func_term = make_counted<func_term_t>(&env, optarg);
        counted_t<const func_t> func =
            func_term->eval_to_func(var_scope_t(), term_storage);

        auto res = optargs.insert(std::make_pair(std::string(optarg_it.optarg_name()),
                                                 wire_func_t(func)));
        rcheck_toplevel(res.second, base_exc_t::GENERIC, strprintf(
            "Duplicate global optional argument: `%s`.",
            optarg_it.optarg_name().c_str()));
    }
}

bool global_optargs_t::has_optarg(const std::string &key) const {
    return optargs.count(key) > 0;
}

scoped_ptr_t<val_t> global_optargs_t::get_optarg(env_t *env, const std::string &key) {
    auto it = optargs.find(key);
    if (it == optargs.end()) {
        return scoped_ptr_t<val_t>();
    }
    return it->second.compile_wire_func()->call(env);
}

static const std::set<std::string> acceptable_optargs({
    "_EVAL_FLAGS_",
    "_NO_RECURSE_",
    "_SHORTCUT_",
    "array_limit",
    "attempts",
    "auth",
    "base",
    "binary_format",
    "conflict",
    "data",
    "db",
    "default",
    "default_timezone",
    "dry_run",
    "durability",
    "fill",
    "first_batch_scaledown_factor",
    "float",
    "geo",
    "geo_system",
    "group_format",
    "header",
    "identifier_format",
    "include_states",
    "index",
    "left_bound",
    "max_batch_bytes",
    "max_batch_rows",
    "max_batch_seconds",
    "max_dist",
    "max_results",
    "method",
    "min_batch_rows",
    "multi",
    "non_atomic",
    "noreply",
    "num_vertices",
    "overwrite",
    "page",
    "page_limit",
    "params",
    "primary_key",
    "primary_replica_tag",
    "profile",
    "redirects",
    "replicas",
    "result_format",
    "return_changes",
    "return_vals",
    "right_bound",
    "shards",
    "squash",
    "time_format",
    "timeout",
    "unit",
    "use_outdated",
    "verify",
    "wait_for",
});

void global_optargs_t::validate_optarg(const std::string &key,
                                       backtrace_id_t bt) {
    rcheck_src(bt, acceptable_optargs.count(key) != 0, base_exc_t::GENERIC,
               strprintf("Unrecognized optional argument `%s`.", key.c_str()));
}

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(global_optargs_t, optargs);

} // namespace ql
