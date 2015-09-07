// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_QUERY_HPP_
#define RDB_PROTOCOL_QUERY_HPP_

#include <map>

#include "errors.hpp"
#include <boost/variant.hpp>

#include "rapidjson/rapidjson.h"
#include "rapidjson/document.h"

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
class raw_term_t;

enum class source_type_t { REAL, GENERATED };

class arg_iterator_t {
public:
    arg_iterator_t(arg_iterator_t &&) = default;
    raw_term_t next();
    bool has_next() const;
private:
    friend class raw_term_t;
    friend class term_storage_t;
    explicit arg_iterator_t(raw_term_t *_parent);

    raw_term_t *parent;
    size_t index;

    DISABLE_COPYING(arg_iterator_t);
};

class optarg_iterator_t {
public:
    optarg_iterator_t(optarg_iterator_t &&) = default;
    raw_term_t next();
    bool has_next() const;
private:
    friend class raw_term_t;
    explicit optarg_iterator_t(source_type_t source);
    explicit optarg_iterator_t(generated_term_storage_t::fake_term_t *_optargs);

    union {
        struct {
            rapidjson::Value *source;
            rapidjson::MemberIterator it;
        } real;
        struct {
            generated_term_storage_t::fake_term_t *source;
            std::map<const char *, generated_term_storage_t::fake_term_t>::iterator_t it;
        } generated;
    };

    source_type_t source_type;

    DISABLE_COPYING(optarg_iterator_t);
};

class real_term_storage_t : public term_storage_t {
public:
    raw_term_t root_term();
    optarg_iterator_t global_optargs();

private:
    // We hold onto this for the lifetime of 'doc' because we use in-situ parsing
    scoped_array_t<char> original_data;
    rapidjson::Document doc;

    const rapidjson::Value *root_term_json;
    const rapidjson::Value *global_optargs_json;
};

// Used by the minidriver_t - may refer back to json in the original real_term_storage_t
class generated_term_storage_t : public term_storage_t {
public:
    raw_term_t root_term();

private:
    friend class raw_term_t;
    struct fake_term_t {
        struct child_term_t {
            source_type_t source_type;
            union {
                fake_term_t fake;
                rapidjson::Value *json;
            }
        };

        Term::TermType term_type;
        std::vector<child_term_t> args;
        std::map<const char *, child_term_t> optargs;
    };

    backtrace_id_t bt;
    fake_term_t root_term;

    // TODO: drainer lock on real_term_storage_t?
};

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
    real_term_storage_t term_storage;

    int64_t token;
    query_id_t id;
    Query::QueryType type;
    bool noreply;
    bool profile;

    new_semaphore_acq_t throttler;
private:
    bool static_optarg_as_bool(const std::string &key, bool default_value);

    DISABLE_COPYING(query_params_t);
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
    raw_term_t(rapidjson::Value *src, const char *_optarg_name);

    Term::TermType type_;
    backtrace_id_t bt_;
    const char *optarg_name_;
    source_type_t source_type;

    union {
        struct generated_t {
            generated_term_storage_t *parent;
            generated_term_storage_t::fake_term_t *fake_term;
        } generated;

        struct {
            rapidjson::Value *args;
            rapidjson::Value *optargs;
            rapidjson::Value *datum;
        } real;
    };
};

// Deserializes a term tree into the term storage, and provides a pointer to
// the root term of the deserialized tree through the root_term_out parameter.
template <cluster_version_t W>
archive_result_t deserialize_term_tree(read_stream_t *s,
                                       real_term_storage_t *root_term_out,
                                       reql_version_t reql_version);

template <cluster_version_t W>
void serialize_term_tree(write_message_t *wm,
                         m_t &root_term);

} // namespace ql

#endif // RDB_PROTOCOL_QUERY_HPP_
