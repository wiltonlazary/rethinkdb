// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_QUERY_HPP_
#define RDB_PROTOCOL_QUERY_HPP_

#include <map>

#include "rapidjson/rapidjson.h"

#include "containers/intrusive_list.hpp"
#include "containers/segmented_vector.hpp"

#include "rdb_protocol/backtrace.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/ql2.pb.h"
#include "rdb_protocol/wire_func.hpp"

namespace ql {

class query_cache_t;

// TODO: change guarantees to sanity checks
// TODO: split term_storage_t out into its own file
struct query_params_t {
public:
    query_params_t(int64_t _token,
                   ql::query_cache_t *_query_cache,
                   rapidjson::Document &&query_json);

    // A query id is allocated when each query is received from the client
    // in order, so order can be checked for in queries that require it
    class query_id_t : public intrusive_list_node_t<query_id_t> {
    public:
        explicit query_id_t(query_cache_t *_parent);
        query_id_t(query_id_t &&other);
        ~query_id_t();

        uint64_t value() const;

    private:
        query_cache_t *parent;
        uint64_t value_;
        DISABLE_COPYING(query_id_t);
    };

    query_cache_t *query_cache;
    rapidjson::Document doc;
    int64_t token;
    query_id_t id;

    Query::QueryType type;
    bool noreply;
    bool profile;

    const rapidjson::Value *root_term_json;
    const rapidjson::Value *global_optargs_json;
private:
    bool static_optarg_as_bool(const std::string &key, bool default_value);
};

class raw_term_t;

class raw_term_iterator_t {
public:
    const raw_term_t *next();
private:
    friend class raw_term_t;
    explicit raw_term_iterator_t(const intrusive_list_t<raw_term_t> *_list);

    const intrusive_list_t<raw_term_t> *list;
    const raw_term_t *item;
};

struct raw_term_t : public intrusive_list_node_t<raw_term_t> {
    size_t num_args() const { return args_.size(); }
    size_t num_optargs() const { return optargs_.size(); }

    raw_term_iterator_t args() const { return raw_term_iterator_t(&args_); }
    raw_term_iterator_t optargs() const { return raw_term_iterator_t(&optargs_); }

    const char *optarg_name() const { return optarg_name_; }
    void set_optarg_name(const std::string &name);

    Term::TermType type;
    backtrace_id_t bt;
    datum_t value; // Only used for datum type

    intrusive_list_t<raw_term_t> args_; // Not used for datum type
    intrusive_list_t<raw_term_t> optargs_; // Not used for datum type

private:
    const char *optarg_name_; // Only used when an optarg
};

class term_storage_t {
public:
    // Parse a query from a rapidjson value and attach backtraces
    const raw_term_t *add_term_tree(const rapidjson::Value &v);
    void add_global_optargs(const rapidjson::Value &v);
    void add_global_optarg(const char *key, const rapidjson::Value &v);

    const raw_term_t *root_term() const {
        r_sanity_check(terms.size() > 0, "No root term has been created.");
        return &terms[0];
    }

    const std::map<std::string, wire_func_t> &global_optargs() const {
        return global_optargs_;
    }

    const backtrace_registry_t &bt_reg() const {
        return backtrace_registry;
    }

private:
    // We use a segmented vector so items won't be reallocated and moved, which allows us
    // to use pointers to other items in the vector.
    segmented_vector_t<raw_term_t, 100> terms;
    std::map<std::string, wire_func_t> global_optargs_;
    backtrace_registry_t backtrace_registry;
    datum_t start_time;

    datum_t get_time();

    raw_term_t *new_term(Term::TermType type, backtrace_id_t bt);

    void add_args(const rapidjson::Value &args,
                  intrusive_list_t<raw_term_t> *args_out,
                  backtrace_registry_t *bt_reg,
                  backtrace_id_t bt);

    void add_optargs(const rapidjson::Value &optargs,
                     intrusive_list_t<raw_term_t> *optargs_out,
                     backtrace_registry_t *bt_reg,
                     backtrace_id_t bt);

    raw_term_t *parse_internal(const rapidjson::Value &v,
                               backtrace_registry_t *bt_reg,
                               backtrace_id_t bt);

    void add_global_optarg_internal(const char *key,
                                    const raw_term_t *term);
};

} // namespace ql

#endif // RDB_PROTOCOL_QUERY_HPP_
