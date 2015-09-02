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
                   scoped_array_t<char> &&_original_data,
                   rapidjson::Document &&_query_json);

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

    void maybe_release_query_id();

    query_cache_t *query_cache;
    rapidjson::Document query_json;
    int64_t token;
    query_id_t id;

    Query::QueryType type;
    bool noreply;
    bool profile;

    const rapidjson::Value *root_term_json;
    const rapidjson::Value *global_optargs_json;

    new_semaphore_acq_t throttler;
private:
    bool static_optarg_as_bool(const std::string &key, bool default_value);

    // We hold onto this for the lifetime of 'doc' because we use in-situ parsing
    scoped_array_t<char> original_data;

    DISABLE_COPYING(query_params_t);
};

class raw_term_t;

class arg_iterator_t {
public:
    arg_iterator_t(arg_iterator_t &&) = default;
    raw_term_t next();
private:
    friend class raw_term_t;
    friend class term_storage_t;
    explicit arg_iterator_t(rapidjson::Value *_args);

    rapidjson::Value *args;
    size_t index;

    DISABLE_COPYING(arg_iterator_t);
};

class optarg_iterator_t {
public:
    optarg_iterator_t(optarg_iterator_t &&) = default;
    const char *optarg_name() const;
    raw_term_t next();
private:
    friend class raw_term_t;
    explicit optarg_iterator_t(rapidjson::Value *_optargs);

    rapidjson::Value *optargs;
    rapidjson::MemberIterator it;

    DISABLE_COPYING(optarg_iterator_t);
};

class raw_term_t {
public:
    size_t num_args() const;
    size_t num_optargs() const;

    arg_iterator_t args() const;
    optarg_iterator_t optargs() const;

    // This parses the datum each time it is called - keep calls to a minimum
    // TODO: cache the result
    datum_t datum() const;

    const char *optarg_name() const;
    Term::TermType type() const;
    backtrace_id_t bt() const;

private:
    friend class term_storage_t;
    friend class minidriver_t;
    raw_term_t(rapidjson::Value *src,
               const char *_optarg_name);

    Term::TermType type_;
    backtrace_id_t bt_;
    rapidjson::Value *args_;
    rapidjson::Value *optargs_;
    rapidjson::Value *datum_;
    const char *optarg_name_;
};

class term_storage_t : public slow_atomic_countable_t<term_storage_t> {
public:
    term_storage_t(rapidjson::Value &&root);
    term_storage_t(term_storage_t &&) = default;

    raw_term_t root_term();
private:
    rapidjson::Value root_term_;
    DISABLE_COPYING(term_storage_t);
};

// Deserializes a term tree into the term storage, and provides a pointer to
// the root term of the deserialized tree through the root_term_out parameter.
template <cluster_version_t W>
archive_result_t deserialize_term_tree(read_stream_t *s,
                                       rapidjson::Document *root_term_out,
                                       reql_version_t reql_version);

template <cluster_version_t W>
void serialize_term_tree(write_message_t *wm,
                         const raw_term_t &root_term);

} // namespace ql

#endif // RDB_PROTOCOL_QUERY_HPP_
