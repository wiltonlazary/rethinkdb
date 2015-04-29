// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_OPTARGS_HPP_
#define RDB_PROTOCOL_OPTARGS_HPP_

#include <map>
#include <string>

#include "containers/counted.hpp"
#include "containers/scoped.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/query.hpp"
#include "rdb_protocol/wire_func.hpp"

namespace ql {
class env_t;
class val_t;

class global_optargs_t {
public:
    global_optargs_t();
    global_optargs_t(global_optargs_t &&) = default;
    global_optargs_t(const global_optargs_t &) = default;
    explicit global_optargs_t(counted_t<term_storage_t> term_storage);

    bool has_optarg(const std::string &key) const;

    // returns NULL if no entry
    scoped_ptr_t<val_t> get_optarg(env_t *env, const std::string &key);

    static const char *validate_optarg(const std::string &key, backtrace_id_t bt);
private:
    std::map<std::string, wire_func_t> optargs;

    RDB_DECLARE_ME_SERIALIZABLE(global_optargs_t);
};

} // namespace ql

#endif // RDB_PROTOCOL_OPTARGS_HPP_
