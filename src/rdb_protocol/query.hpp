// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_QUERY_HPP_
#define RDB_PROTOCOL_QUERY_HPP_

#include <map>

#include "errors.hpp"
#include <boost/variant.hpp>

#include "rapidjson/rapidjson.h"

#include "containers/counted.hpp"
#include "containers/intrusive_list.hpp"
#include "containers/segmented_vector.hpp"
#include "rdb_protocol/backtrace.hpp"
#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/ql2.pb.h"
#include "version.hpp"

namespace ql {

class query_cache_t;

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

    DISABLE_COPYING(query_params_t);
};

class raw_term_t;

class raw_term_iterator_t {
public:
    raw_term_iterator_t(raw_term_iterator_t &&) = default;
    const raw_term_t *next();
private:
    friend class raw_term_t;
    friend class term_storage_t;
    explicit raw_term_iterator_t(const intrusive_list_t<raw_term_t> *_list);

    const intrusive_list_t<raw_term_t> *list;
    const raw_term_t *item;

    DISABLE_COPYING(raw_term_iterator_t);
};

struct raw_term_t : public intrusive_list_node_t<raw_term_t> {
    raw_term_t();

    size_t num_args() const;
    size_t num_optargs() const;

    raw_term_iterator_t args() const;
    raw_term_iterator_t optargs() const;

    const datum_t &datum() const;

    const char *optarg_name() const;
    void set_optarg_name(const std::string &name);

    Term::TermType type;
    backtrace_id_t bt; // TODO: this isn't needed if we're a reference

private:
    static const Term::TermType REFERENCE;

    friend class raw_term_iterator_t;
    friend class term_storage_t;
    friend class minidriver_t;

    void reset(Term::TermType type);

    datum_t &mutable_datum();
    const raw_term_t *&mutable_ref();
    intrusive_list_t<raw_term_t> &mutable_args();
    intrusive_list_t<raw_term_t> &mutable_optargs();

    intrusive_list_t<raw_term_t> args_; // Used for almost all Term types
    intrusive_list_t<raw_term_t> optargs_; // Used for almost all Term types
    datum_t value; // Used for DATUM terms
    const raw_term_t *src; // Used for REF terms

    const char *optarg_name_; // Only used when an optarg
    DISABLE_COPYING(raw_term_t);
};

class term_storage_t : public slow_atomic_countable_t<term_storage_t> {
public:
    term_storage_t();
    term_storage_t(term_storage_t &&) = default;
    ~term_storage_t();

    // Parse a query from a rapidjson value and attach backtraces
    void add_root_term(const rapidjson::Value &v);
    void add_global_optargs(const rapidjson::Value &v);

    const raw_term_t *root_term() const {
        r_sanity_check(terms.size() > 0, "No root term has been created.");
        return &terms[0];
    }

    raw_term_iterator_t global_optargs() const {
        return raw_term_iterator_t(&global_optarg_list);
    }

    const backtrace_registry_t &bt_reg() const {
        return backtrace_registry;
    }

    // Deserializes a term tree into the term storage, and provides a pointer to
    // the root term of the deserialized tree through the root_term_out parameter.
    template <cluster_version_t W>
    archive_result_t deserialize_term_tree(read_stream_t *s,
                                           raw_term_t **root_term_out,
                                           reql_version_t reql_version);

private:
    // We use a segmented vector so items won't be reallocated and moved, which allows us
    // to use pointers to other items in the vector.
    segmented_vector_t<raw_term_t, 100> terms;
    intrusive_list_t<raw_term_t> global_optarg_list;
    backtrace_registry_t backtrace_registry;
    datum_t start_time;

    datum_t get_time();

    friend class minidriver_t;
    raw_term_t *new_term(Term::TermType type, backtrace_id_t bt);
    raw_term_t *new_ref(const raw_term_t *src);

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

    raw_term_t *parse_internal(const Term &term,
                               reql_version_t reql_version);

    DISABLE_COPYING(term_storage_t);
};

template <cluster_version_t W>
void serialize_term_tree(write_message_t *wm,
                         const raw_term_t *root_term);

} // namespace ql

#endif // RDB_PROTOCOL_QUERY_HPP_
